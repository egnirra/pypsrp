# -*- coding: utf-8 -*-
# Copyright: (c) 2021, Jordan Borean (@jborean93) <jborean93@gmail.com>
# MIT License (see LICENSE or https://opensource.org/licenses/MIT)

import asyncio
from asyncio import subprocess
from asyncio.transports import Transport
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
        # self._protocol = protocol
        self._closing = False
        self._paused = False

    def close(self) -> None:
        self._closing = True

    def is_closing(self) -> bool:
        return self._closing

    def set_protocol(
        self,
        protocol: asyncio.Protocol,
    ) -> None:
        self._protocol = protocol

    def get_protocol(self) -> asyncio.Protocol:
        return self._protocol

    def is_reading(self) -> bool:
        raise NotImplementedError()

    def pause_reading(self) -> None:
        raise NotImplementedError()

    def resume_reading(self) -> None:
        raise NotImplementedError()


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


# class WinRSWritePipeProtocol(asyncio.Protocol):
#     def __init__(
#         self,
#         fd: int,
#     ) -> None:
#         self.transport = None
#         self.fd = fd
#         self.disconnected = False

#     def connection_made(
#         self,
#         transport: asyncio.Transport,
#     ):
#         self.transport = transport

#     def connection_lost(
#         self,
#         exc: typing.Optional[Exception],
#     ) -> None:
#         raise NotImplementedError()

#     def pause_writing(self) -> None:
#         raise NotImplementedError()

#     def resume_writing(self) -> None:
#         raise NotImplementedError()


# class WinRSReadPipeProtocol(WinRSWritePipeProtocol):
#     def data_received(
#         self,
#         data: bytes,
#     ) -> None:
#         raise NotImplementedError()


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
        self._process_exited = asyncio.Future()

        loop.create_task(self._listen(waiter))

    def set_protocol(
        self,
        protocol: asyncio.SubprocessProtocol,
    ) -> None:
        self._protocol = protocol

    def get_protocol(self) -> asyncio.SubprocessProtocol:
        return self._protocol

    def get_pid(self) -> int:
        raise NotImplementedError()

    def get_pipe_transport(self, fd: int) -> None:
        return self._pipes.get(fd, None)

    def get_returncode(self) -> typing.Optional[int]:
        return self._returncode

    def kill(self) -> None:
        raise NotImplementedError()

    def send_signal(self, signal: int) -> int:
        raise NotImplementedError()

    def terminate(self) -> None:
        raise NotImplementedError()

    def close(self) -> None:
        raise NotImplementedError()

    async def _wait(self) -> int:
        await self._process_exited
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

        self._process_exited.set_result(None)
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


class AsyncWinRSProcess:
    def __init__(
        self,
        winrs: AsyncWinRS,
        executable: str,
        args: typing.Optional[typing.List[str]] = None,
        no_shell: bool = False,
    ):
        self.executable = executable
        self.args = args
        self.no_sell = no_shell
        self._stdin_r = self._stdout_w = self._stderr_w = self.stdin = self.stdout = self.stdin = None
        # self.pid = None
        self.returncode = None

        self._command_id = None
        self._state = CommandState.pending
        self._winrs = winrs
        self._receive_task = None

    async def __aenter__(self):
        await self.start()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.terminate()
        await self._receive_task
        self._receive_task = None

    async def poll(
        self,
    ) -> typing.Optional[int]:
        a = ""

    async def wait(
        self,
        timeout: typing.Optional[int] = None,
    ) -> int:
        await self._receive_task

    async def communicate(
        self,
        input_data: typing.Optional[bytes] = None,
        timeout: typing.Optional[int] = None,
    ) -> typing.Tuple[bytes, bytes]:
        self.stdin.write(input_data)
        await self.stdin.drain()

    async def send_signal(
        self,
        signal: SignalCode,
    ):
        self._winrs.winrs.signal(signal, self._command_id)
        await self._winrs._exchange_data()

    async def start(
        self,
    ):
        loop = asyncio.get_event_loop()
        self.stdout = asyncio.StreamReader(loop=loop)
        self.stderr = asyncio.StreamReader(loop=loop)

        r = asyncio.StreamReader(loop=loop)
        p = asyncio.StreamReaderProtocol(r, loop=loop)
        t = None

        self.stdin = asyncio.StreamWriter(transport=t, protocol=p, reader=r, loop=loop)

        self._winrs.winrs.command(self.executable, args=self.args, no_shell=self.no_sell)
        command_event = await self._winrs._exchange_data()
        self._state = CommandState.running
        self._command_id = command_event.command_id
        self._receive_task = asyncio.create_task(self._receive())

    async def terminate(
        self,
    ):
        await self.send_signal(SignalCode.terminate)

    async def kill(
        self,
    ):
        await self.send_signal(SignalCode.ctrl_c)

    async def _receive(
        self,
    ):
        # Use a new WSMan connection so we can send the Receive requests in parallel to the main shell connection.
        async with AsyncWSManConnection(self._winrs.winrs.wsman.connection_uri) as io:
            while self._state != CommandState.done:
                self._winrs.winrs.receive(command_id=self._command_id)

                try:
                    receive_response = await self._winrs._exchange_data(io=io)
                except OperationTimedOut:
                    # Expected if no data was available in the WSMan operational_timeout time. Just send the receive
                    # request again until there is data available.
                    continue

                if receive_response.exit_code is not None:
                    self.returncode = receive_response.exit_code
                self._state = receive_response.command_state

                buffer = receive_response.get_streams()
                pipe_map = [("stdout", self.stdout), ("stderr", self.stderr)]
                for name, pipe in pipe_map:
                    for data in buffer.get(name, []):
                        pipe.feed_data(data)

        self.stdout.feed_eof()
        self.stderr.feed_eof()


class StdinTransport(asyncio.transports.WriteTransport):
    def __init__(self, loop, protocol):
        super().__init__()
        self._loop = loop
        self._protocol = protocol

    def write(self, data):
        """Write some data bytes to the transport.

        This does not block; it buffers the data and arranges for it
        to be sent out asynchronously.
        """
        self._protocol.data_received(data)

    def writelines(self, list_of_data):
        """Write a list (or any iterable) of data bytes to the transport.

        The default implementation concatenates the arguments and
        calls write() on the result.
        """
        data = b"".join(list_of_data)
        self.write(data)

    def write_eof(self):
        """Close the write end after flushing buffered data.

        (This is like typing ^D into a UNIX program reading from stdin.)

        Data may still be received.
        """
        self._protocol.eof_received()

    def can_write_eof(self):
        """Return True if this transport supports write_eof(), False if not."""
        return True

    def abort(self):
        """Close the transport immediately.

        Buffered data will be lost.  No more data will be received.
        The protocol's connection_lost() method will (eventually) be
        called with None as its argument.
        """
        raise NotImplementedError


def _create_inmemory_stream():
    loop = asyncio.events.get_event_loop()

    reader = asyncio.StreamReader(loop=loop)
    protocol = asyncio.StreamReaderProtocol(reader, loop=loop)
    transport = StdinTransport(loop, protocol)
    # transport.set_write_buffer_limits(0)  # Make sure .drain() actually sends all the data.
    writer = asyncio.StreamWriter(transport, protocol, reader, loop)

    return reader, writer
