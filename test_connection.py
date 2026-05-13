#TEST
import asyncio
from database import connect_to_rdb, connect_to_mongo, connect_to_es

async def test():
    await connect_to_rdb()
    await connect_to_mongo()
    await connect_to_es()

asyncio.run(test())