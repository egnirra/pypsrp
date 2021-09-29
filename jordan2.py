import asyncio

from psrp.winrs import *


async def main():
    a = await asyncio.create_subprocess_exec("whoami")
    a = ""

    # async with AsyncWinRS("http://server2019.domain.test:5985/wsman") as winrs:
    #     await winrs.create()
    #     proc = winrs.execute("whoami.exe")
    #     async with proc:
    #         await proc.wait()
    #         a = ""


asyncio.run(main())
