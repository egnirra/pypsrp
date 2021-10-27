# -*- coding: utf-8 -*-
# Copyright: (c) 2021, Jordan Borean (@jborean93) <jborean93@gmail.com>
# MIT License (see LICENSE or https://opensource.org/licenses/MIT)

import asyncio
import logging
import subprocess
import typing

from psrp._out_of_proc import (
    AsyncOutOfProcInfo,
    OutOfProcInfo,
)


log = logging.getLogger(__name__)


class ProcessInfo(OutOfProcInfo):
    """ConnectionInfo for a Process.

    ConnectionInfo implementation for a native process. The data is read from
    the ``stdout`` pipe of the process and the input is read to the ``stdin``
    pipe. This can be used to create a Runspace Pool on a local PowerShell
    instance or any other process that can handle the raw PSRP OutOfProc
    messages.

    Args:
        executable: The executable to run, defaults to `pwsh`.
        arguments: A list of arguments to run, when the executable is `pwsh`
            then this defaults to `-NoProfile -NoLogo -s`.
    """

    def __init__(
        self,
        executable: str = "pwsh",
        arguments: typing.Optional[typing.List[str]] = None,
    ) -> None:
        super().__init__()

        self.executable = executable
        self.arguments = arguments or []
        if executable == "pwsh" and arguments is None:
            self.arguments = ["-NoProfile", "-NoLogo", "-s"]

        self._process: subprocess.Popen = None

    def read(self) -> typing.Optional[bytes]:
        return self._process.stdout.read(32_768)

    def write(
        self,
        data: bytes,
    ) -> None:
        self._process.stdin.write(data)
        self._process.stdin.flush()

    def start(self) -> None:
        pipe = subprocess.PIPE
        arguments = [self.executable]
        arguments.extend(self.arguments)

        self._process = subprocess.Popen(arguments, stdin=pipe, stdout=pipe, stderr=subprocess.STDOUT)

    def stop(self) -> None:
        if self._process.poll() is None:
            self._process.kill()
            self._process.wait()


class AsyncProcessInfo(AsyncOutOfProcInfo):
    """Async ConnectionInfo for a Process.

    Async ConnectionInfo implementation for a native process. The data is read
    from the ``stdout`` pipe of the process and the input is read to the
    ``stdin`` pipe. This can be used to create a Runspace Pool on a local
    PowerShell instance or any other process that can handle the raw PSRP
    OutOfProc messages.

    Args:
        executable: The executable to run, defaults to `pwsh`.
        arguments: A list of arguments to run, when the executable is `pwsh`
            then this defaults to `-NoProfile -NoLogo -s`.
    """

    def __init__(
        self,
        executable: str = "pwsh",
        arguments: typing.Optional[typing.List[str]] = None,
    ) -> None:
        super().__init__()

        self.executable = executable
        self.arguments = arguments or []
        if executable == "pwsh" and arguments is None:
            self.arguments = ["-NoProfile", "-NoLogo", "-s"]

        self._process: typing.Optional[asyncio.subprocess.Process] = None

    async def read(self) -> typing.Optional[bytes]:
        return await self._process.stdout.read(32_768)

    async def write(
        self,
        data: bytes,
    ):
        self._process.stdin.write(data)
        await self._process.stdin.drain()

    async def start(self) -> None:
        pipe = subprocess.PIPE
        self._process = await asyncio.create_subprocess_exec(
            self.executable,
            *self.arguments,
            stdin=pipe,
            stdout=pipe,
            stderr=subprocess.STDOUT,
            limit=32_768,
        )

    async def stop(self) -> None:
        if self._process:
            self._process.kill()
            await self._process.wait()
            self._process = None
