# -*- coding: utf-8 -*-
# Copyright: (c) 2021, Jordan Borean (@jborean93) <jborean93@gmail.com>
# MIT License (see LICENSE or https://opensource.org/licenses/MIT)

import asyncio
import signal as py_signal
import subprocess
import typing

from psrp.exceptions import (
    OperationTimedOut,
)

from psrp.io.wsman import (
    AsyncWSManConnection,
)

from psrp.protocol.winrs import (
    SignalCode,
    WinRS,
)

from psrp.protocol.wsman import (
    CommandState,
    WSMan,
)


# Signals known to WinRS, some are platform specific so have a fallback value
SIGINT = py_signal.SIGINT
SIGKILL = getattr(py_signal, "SIGKILL", py_signal.Signals(9))
SIGTERM = py_signal.SIGTERM
SIGBREAK = getattr(py_signal, "SIGBREAK", py_signal.Signals(21))


class AsyncWinRS:
    """A Windows Remote Shell - Async.

    Represents an opened Shell that is managed over WinRM/WSMan. This is the async variant that is designed to run with
    asyncio for faster concurrent operations.
    """

    def __init__(
        self,
        connection_uri: str,
        codepage: typing.Optional[int] = None,
        environment: typing.Optional[typing.Dict[str, str]] = None,
        idle_time_out: typing.Optional[int] = None,
        lifetime: typing.Optional[int] = None,
        no_profile: typing.Optional[bool] = None,
        working_directory: typing.Optional[str] = None,
    ):
        self.connection_uri = connection_uri
        wsman = WSMan(connection_uri)
        self.winrs = WinRS(
            wsman,
            codepage=codepage,
            environment=environment,
            idle_time_out=idle_time_out,
            lifetime=lifetime,
            no_profile=no_profile,
            working_directory=working_directory,
        )
        self._io = AsyncWSManConnection(connection_uri)

    async def __aenter__(self):
        await self._io.open()
        await self.create()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()
        await self._io.close()

    async def close(self):
        """Closes the WinRS shell."""
        self.winrs.close()
        await self._exchange_data()

    async def create(self):
        """Opens the WinRS shell."""
        self.winrs.open()
        await self._exchange_data()

    async def create_subprocess(
        self,
        program: str,
        *args: str,
        stdin: typing.Optional[typing.Union[int]] = subprocess.PIPE,
        stdout: typing.Optional[typing.Union[int]] = subprocess.PIPE,
        stderr: typing.Optional[typing.Union[int]] = subprocess.PIPE,
        limit: int = 65536,
        **kwds: typing.Any,
    ) -> "AsyncWinRSProcess":
        loop = asyncio.get_event_loop()

        protocol_factory = lambda: WinRSSubprocessProtocol(
            limit=limit,
            loop=loop,
            stdin=stdin,
            stdout=stdout,
            stderr=stderr,
        )
        transport, protocol = await self.subprocess_exec(protocol_factory, program, *args, **kwds)
        return WinRSProcess(transport, protocol, loop)

    async def subprocess_exec(
        self,
        protocol_factory: typing.Callable[[], asyncio.SubprocessProtocol],
        program: str,
        *args: str,
        no_shell: bool = False,
    ) -> typing.Tuple[asyncio.SubprocessTransport, asyncio.SubprocessProtocol]:
        loop = asyncio.get_event_loop()

        self.winrs.command(program, args=args, no_shell=no_shell)
        command_event = await self._exchange_data()

        waiter = loop.create_future()
        protocol = protocol_factory()
        transport = WinRSProcessTransport(
            loop,
            protocol,
            shell=self,
            command_id=command_event.command_id,
            waiter=waiter,
        )

        try:
            await waiter
        except (SystemExit, KeyboardInterrupt):
            raise
        except BaseException:
            transport.close()
            await transport._wait()
            raise

        return transport, protocol

    async def _exchange_data(self, io=None):
        """Sends the pending messages from the WinRS shell over the IO object and returns the response."""
        if not io:
            io = self._io

        content = self.winrs.data_to_send()
        if not content:
            return

        response = await io.send(content)
        event = self.winrs.receive_data(response)
        return event


class WinRSReadPipeTransport(asyncio.ReadTransport):
    def __init__(
        self,
        transport: "WinRSProcessTransport",
        extra: typing.Optional[dict] = None,
    ) -> None:
        super().__init__(extra)
        self._transport = transport
        self._closing = False

    def close(self) -> None:
        self._closing = True

    def is_closing(self) -> bool:
        return self._closing


class WinRSWritePipeTransport(asyncio.WriteTransport):
    def __init__(
        self,
        transport: "WinRSProcessTransport",
        extra: typing.Optional[dict] = None,
    ) -> None:
        super().__init__(extra)
        self._transport = transport
        # self._protocol = protocol
        self._conn_lost = 0
        self._closing = False  # Set when close() or write_eof() called.

    def abort(self) -> None:
        raise NotImplementedError()

    def can_write_eof(self) -> bool:
        return True

    def get_write_buffer_size(self) -> int:
        raise NotImplementedError()

    def get_write_buffer_limits(self) -> typing.Tuple[int, int]:
        raise NotImplementedError()

    def set_write_buffer_limits(
        self,
        high: typing.Optional[int] = None,
        low: typing.Optional[int] = None,
    ) -> None:
        raise NotImplementedError()

    def write(
        self,
        data: typing.Union[bytes, bytearray, memoryview],
    ) -> None:
        raise NotImplementedError()

    def writelines(
        self,
        data: typing.List[typing.Union[bytes, bytearray, memoryview]],
    ) -> None:
        raise NotImplementedError()

    def write_eof(self) -> None:
        raise NotImplementedError()


class WinRSProcessTransport(asyncio.SubprocessTransport):
    def __init__(
        self,
        loop: asyncio.BaseEventLoop,
        protocol: asyncio.SubprocessProtocol,
        shell: AsyncWinRS,
        command_id: str,
        waiter: typing.Optional[asyncio.Future] = None,
        extra: typing.Optional[dict] = None,
    ) -> None:
        super().__init__(extra)
        self._loop = loop
        self._protocol = protocol
        self._returncode: typing.Optional[int] = None
        self._finished = False
        self._shell = shell
        self._command_id = command_id
        self._pipes: typing.Dict[int, typing.Union[WinRSReadPipeTransport, WinRSWritePipeTransport]] = {
            0: WinRSWritePipeTransport(self),
            1: WinRSReadPipeTransport(self),
            2: WinRSReadPipeTransport(self),
        }
        self._end_waiters: typing.List[asyncio.Future] = []

        loop.create_task(self._listen(waiter))

    def set_protocol(
        self,
        protocol: asyncio.SubprocessProtocol,
    ) -> None:
        self._protocol = protocol

    def get_protocol(self) -> asyncio.SubprocessProtocol:
        return self._protocol

    def get_pipe_transport(self, fd: int) -> None:
        return self._pipes.get(fd, None)

    def get_returncode(self) -> typing.Optional[int]:
        return self._returncode

    def kill(self) -> None:
        self.terminate()

    def send_signal(
        self,
        signal: typing.Union[int, str],
    ) -> int:
        if signal == SIGINT:
            signal = SignalCode.ctrl_c
        elif signal == SIGBREAK:
            signal = SignalCode.ctrl_break
        elif signal in [SIGKILL, SIGTERM]:
            signal = SignalCode.terminate
        elif isinstance(signal, int):
            raise ValueError(f"invalid signal, must be {SIGINT!s}, {SIGKILL!s}, {SIGTERM!s}, or {SIGBREAK!s}")

        self._shell.winrs.signal(signal, self._command_id)
        self._loop.call_soon(self._shell._exchange_data)

    def terminate(self) -> None:
        self.send_signal(SIGTERM)

    def close(self) -> None:
        if self._closed:
            return

        self.terminate()
        self._closed = True

    async def _wait(self) -> int:
        waiter = self._loop.create_future()
        self._end_waiters.append(waiter)
        await waiter

        return self._returncode

    async def _listen(
        self,
        waiter: typing.Optional[asyncio.Future],
    ):
        # Use a new WSMan connection so we can send the Receive requests in parallel to the main shell connection.
        state = CommandState.running
        async with AsyncWSManConnection(self._shell.connection_uri) as io:
            self._loop.call_soon(self._protocol.connection_made, self)

            if waiter and not waiter.cancelled():
                waiter.set_result(None)

            while state != CommandState.done:
                self._shell.winrs.receive(command_id=self._command_id)

                try:
                    receive_response = await self._shell._exchange_data(io=io)
                except OperationTimedOut:
                    # Expected if no data was available in the WSMan operational_timeout time. Just send the receive
                    # request again until there is data available.
                    continue

                state = receive_response.command_state
                if receive_response.exit_code is not None:
                    self._returncode = receive_response.exit_code

                buffer = receive_response.get_streams()
                for name, fd in [("stdout", 1), ("stderr", 2)]:
                    for line in buffer[name]:
                        self._loop.call_soon(self._protocol.pipe_data_received, fd, line)

            self._loop.call_soon(self._protocol.pipe_connection_lost, 1, None)
            self._loop.call_soon(self._protocol.pipe_connection_lost, 2, None)

            self._shell.winrs.signal(SignalCode.terminate, self._command_id)
            await self._shell._exchange_data(io=io)

        for end_waiter in self._end_waiters:
            end_waiter.set_result(None)
        self._end_waiters = []

        self._loop.call_soon(self._protocol.process_exited)


class WinRSSubprocessProtocol(asyncio.SubprocessProtocol):
    def __init__(
        self,
        limit: int,
        loop: asyncio.BaseEventLoop,
        stdin: typing.Optional[typing.Union[int]] = subprocess.PIPE,
        stdout: typing.Optional[typing.Union[int]] = subprocess.PIPE,
        stderr: typing.Optional[typing.Union[int]] = subprocess.PIPE,
    ):
        self._loop = loop
        self._limit = limit
        self._transport: typing.Optional[asyncio.SubprocessTransport] = None
        self._process_exited = False

        self.stdin: typing.Optional[asyncio.StreamWriter] = None
        self.stdout: typing.Optional[asyncio.StreamReader] = None
        self.stderr: typing.Optional[asyncio.StreamReader] = None

    def connection_made(
        self,
        transport: WinRSProcessTransport,
    ) -> None:
        self._transport = transport

        stdin_transport = transport.get_pipe_transport(0)
        if stdin_transport is not None:
            self.stdin = asyncio.StreamWriter(stdin_transport, protocol=self, reader=None, loop=self._loop)

        stdout_transport = transport.get_pipe_transport(1)
        if stdout_transport is not None:
            self.stdout = asyncio.StreamReader(limit=self._limit, loop=self._loop)
            self.stdout.set_transport(stdout_transport)

        stderr_transport = transport.get_pipe_transport(2)
        if stderr_transport is not None:
            self.stderr = asyncio.StreamReader(limit=self._limit, loop=self._loop)
            self.stderr.set_transport(stderr_transport)

    def pipe_data_received(
        self,
        fd: int,
        data: bytes,
    ):
        """Called when the subprocess writes data into stdout/stderr pipe.

        fd is int file descriptor.
        data is bytes object.
        """
        pipe = {
            1: self.stdout,
            2: self.stderr,
        }.get(fd, None)
        if pipe:
            pipe.feed_data(data)

    def pipe_connection_lost(
        self,
        fd: int,
        exc: typing.Optional[Exception],
    ):
        """Called when a file descriptor associated with the child process is
        closed.

        fd is the int file descriptor that was closed.
        """
        if fd == 0:
            if self.stdin:
                self.stdin.close()

            self.stdin = None
            self.connection_lost(exc)

        pipe = None
        if fd == 1 and self.stdout:
            pipe = self.stdout
            self.stdout = None

        if fd == 2 and self.stderr:
            pipe = self.stderr
            self.stderr = None

        if pipe:
            if not exc:
                pipe.feed_eof()
            else:
                pipe.set_exception(exc)

        self._maybe_close_transport()

    def process_exited(self):
        """Called when subprocess has exited."""
        self._process_exited = True
        self._maybe_close_transport()

    def _maybe_close_transport(self):
        if self._transport and self._process_exited and not self.stdout and not self.stderr:
            self._transport.close()
            self._transport = None


class WinRSProcess:
    def __init__(
        self,
        transport: asyncio.SubprocessTransport,
        protocol: WinRSSubprocessProtocol,
        loop: asyncio.BaseEventLoop,
    ):
        self._transport = transport
        self._protocol = protocol
        self._loop = loop

        self.state = CommandState.running
        self.stdin = protocol.stdin
        self.stdout = protocol.stdout
        self.stderr = protocol.stderr
        # self.pid = transport.get_pid()
        self.returncode: typing.Optional[int] = None

    async def wait(self) -> int:
        return await self._transport._wait()

    async def communicate(
        self,
        input: typing.Optional[bytes] = None,
    ) -> typing.Tuple[typing.Optional[bytes], typing.Optional[bytes]]:
        raise NotImplementedError()

    def send_signal(self, signal: int) -> None:
        raise NotImplementedError()

    def terminate(self) -> None:
        raise NotImplementedError()

    def kill(self) -> None:
        raise NotImplementedError()
