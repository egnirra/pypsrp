import asyncio

from psrp.winrs import *


async def main():
    async with AsyncWinRS("http://server2019.domain.test:5985/wsman") as winrs:
        await winrs.create()
        proc = await winrs.create_subprocess("powershell.exe sleep 30", ";", '"testing"', '\\"abc\\"')
        await proc.wait()
        a = ""


asyncio.run(main())
