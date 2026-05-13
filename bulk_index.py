import asyncio
from database import AsyncSessionLocal, es
from sqlalchemy import text

async def bulk_index():
    async with AsyncSessionLocal() as db:
        result = await db.execute(text("SELECT g.game_id, g.game_name, g.header_image_url, g.game_is_free FROM games g"))
        rows = result.fetchall()
        
        for i, row in enumerate(rows):
            await es.index(
                index="games",
                id=row.game_id,
                document={
                    "game_id": row.game_id,
                    "game_name": row.game_name,
                    "header_image_url": row.header_image_url,
                    "is_free": bool(row.game_is_free)
                }
            )
            if i % 1000 == 0:
                print(f"  진행중... {i}/{len(rows)}")
        
        print(f"✅ {len(rows)}개 게임 ES 인덱싱 완료!")

asyncio.run(bulk_index())