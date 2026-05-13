import asyncio
from database import AsyncSessionLocal, es
from sqlalchemy import text

async def bulk_index():
    async with AsyncSessionLocal() as db:
        result = await db.execute(text("""
            SELECT 
                g.game_id,
                g.game_name,
                g.header_image_url,
                g.game_is_free,
                GROUP_CONCAT(DISTINCT ge.genre_name) AS genres,
                GROUP_CONCAT(DISTINCT d.developer_name) AS developers,
                GROUP_CONCAT(DISTINCT p.publisher_name) AS publishers
            FROM games g
            LEFT JOIN game_genres gg ON g.game_id = gg.game_id
            LEFT JOIN genres ge ON gg.genre_id = ge.genre_id
            LEFT JOIN game_developers gd ON g.game_id = gd.game_id
            LEFT JOIN developers d ON gd.developer_id = d.developer_id
            LEFT JOIN game_publishers gp ON g.game_id = gp.game_id
            LEFT JOIN publishers p ON gp.publisher_id = p.publisher_id
            GROUP BY 
                g.game_id,
                g.game_name,
                g.header_image_url,
                g.game_is_free
        """))

        rows = result.fetchall()

        for i, row in enumerate(rows):
            await es.index(
                index="games",
                id=row.game_id,
                document={
                    "game_id": row.game_id,
                    "game_name": row.game_name,
                    "header_image_url": row.header_image_url,
                    "is_free": bool(row.game_is_free),
                    "genres": row.genres.split(",") if row.genres else [],
                    "developers": row.developers.split(",") if row.developers else [],
                    "publishers": row.publishers.split(",") if row.publishers else []
                }
            )

            if i % 1000 == 0:
                print(f"진행중... {i}/{len(rows)}")

        print(f"✅ {len(rows)}개 게임 ES 인덱싱 완료!")

asyncio.run(bulk_index())