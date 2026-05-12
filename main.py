# main.py
import sys
import asyncio
import re
import argparse

from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException, Depends
from fastapi.middleware.cors import CORSMiddleware
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from datetime import datetime
from collections import Counter

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text

from scrapers import fetch_hana_bank_rates, fetch_all_steam_rankings
from services.itad_api import sync_itad_price_history
from services.steam_api import (
    fetch_full_steam_data, insert_full_game_data,
    fetch_steam_news_only, fetch_steam_reviews_only,
    save_game_reviews_to_mongo, save_game_news_to_mongo
)
from database import AsyncSessionLocal, connect_to_mongo, close_mongo_connection, get_mongodb, get_rdb
from services.stream_tasks import update_chzzk_rank, update_twitch_rank
from store import LATEST_RATES, LATEST_STEAM_RANKS, PENDING_QUEUE, PLATFORM_RANKINGS, LIVE_STREAMS

# =========================================================
# 인자 파싱 (--crawl 플래그)
# =========================================================
parser = argparse.ArgumentParser()
parser.add_argument("--crawl", action="store_true", help="크롤러 모드로 실행")
args, _ = parser.parse_known_args()

IS_CRAWLER = args.crawl

# =========================================================
# 공통: 스케줄 작업 함수
# =========================================================
async def process_hana_bank():
    print(f"\n[{datetime.now()}] 💰 --- 환율 업데이트 시작 ---")
    hana_data = await fetch_hana_bank_rates()
    if hana_data:
        LATEST_RATES["standard_usd"] = hana_data.get("standard_usd", 0.0)
        LATEST_RATES["standard_jpy"] = hana_data.get("standard_jpy", 0.0)
        LATEST_RATES["last_updated"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f"  ✅ USD: {LATEST_RATES['standard_usd']}, JPY: {LATEST_RATES['standard_jpy']}")
    else:
        print("  ❌ 환율 수집 실패")


async def process_steam_rankings():
    steam_data = await fetch_all_steam_rankings()
    if steam_data:
        LATEST_STEAM_RANKS["KR"] = steam_data["KR"]
        LATEST_STEAM_RANKS["JP"] = steam_data["JP"]
        LATEST_STEAM_RANKS["US"] = steam_data["US"]
        LATEST_STEAM_RANKS["last_updated"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f"  ✅ 스팀 랭킹 업데이트 완료 (밴 {steam_data['banned_count']}개 제외)")


async def process_memory_queue():
    """PENDING_QUEUE에 쌓인 게임 재수집 — 크롤러 전용"""
    if not PENDING_QUEUE:
        return

    target_items = sorted(
        [(k, v) for k, v in PENDING_QUEUE.items() if v['retry_count'] < 5],
        key=lambda x: x[1]['last_attempt']
    )[:5]

    if not target_items:
        return

    print(f"\n[{datetime.now()}] 🛒 메모리 대기열 처리 ({len(target_items)}개)")
    async with AsyncSessionLocal() as db:
        for appid, meta in target_items:
            full_info = await fetch_full_steam_data(appid)
            if full_info:
                await insert_full_game_data(db, full_info)
                del PENDING_QUEUE[appid]
                print(f"  ✅ AppID {appid} 저장 완료")
            else:
                PENDING_QUEUE[appid]['retry_count'] += 1
                PENDING_QUEUE[appid]['last_attempt'] = datetime.now()
                print(f"  ⚠️ AppID {appid} 실패 ({PENDING_QUEUE[appid]['retry_count']}회)")
            await asyncio.sleep(2.0)


# =========================================================
# 크롤러 모드 lifespan
# =========================================================
@asynccontextmanager
async def crawler_lifespan(app: FastAPI):
    await connect_to_mongo()

    # 초기 수집
    await process_hana_bank()
    await process_steam_rankings()

    scheduler = AsyncIOScheduler()
    scheduler.add_job(process_hana_bank,       'cron', minute='*/5')
    scheduler.add_job(process_steam_rankings,  'cron', minute='0')
    scheduler.add_job(update_chzzk_rank,       'cron', hour='14,20',      minute=0)
    scheduler.add_job(update_twitch_rank,      'cron', hour='2,8,14,20',  minute=0)
    scheduler.add_job(process_memory_queue,    'interval', minutes=5)
    scheduler.start()

    asyncio.create_task(update_chzzk_rank())
    asyncio.create_task(update_twitch_rank())

    yield
    scheduler.shutdown()
    await close_mongo_connection()


# =========================================================
# 백엔드 모드 lifespan
# =========================================================
@asynccontextmanager
async def backend_lifespan(app: FastAPI):
    await connect_to_mongo()
    yield
    await close_mongo_connection()


# =========================================================
# FastAPI 앱 생성 (모드에 따라 lifespan 분기)
# =========================================================
app = FastAPI(
    title="CG20 API" + (" [CRAWLER]" if IS_CRAWLER else " [BACKEND]"),
    lifespan=crawler_lifespan if IS_CRAWLER else backend_lifespan
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"]
)


# =========================================================
# 공통 엔드포인트
# =========================================================
@app.get("/")
def read_root():
    mode = "CRAWLER" if IS_CRAWLER else "BACKEND"
    return {"msg": f"StreamRank Server is running! [{mode}] 🚀"}


# =========================================================
# [크롤러 전용] 내부 캐시 API (백엔드가 여기서 캐시 조회)
# =========================================================
if IS_CRAWLER:
    @app.get("/cache/rates")
    def get_cache_rates():
        """백엔드 → 크롤러: 환율 캐시 조회"""
        if LATEST_RATES["last_updated"] is None:
            return {"status": "pending", "msg": "환율 수집 중입니다."}
        return {
            "status": "success",
            "data": {
                "usd": LATEST_RATES["standard_usd"],
                "jpy": LATEST_RATES["standard_jpy"]
            },
            "updated_at": LATEST_RATES["last_updated"]
        }

    @app.get("/cache/steam-ranks/{country}")
    def get_cache_steam_ranks(country: str):
        """백엔드 → 크롤러: 스팀 랭킹 캐시 조회"""
        country_upper = country.upper()
        if country_upper not in ["KR", "JP", "US"]:
            raise HTTPException(status_code=400, detail="KR, JP, US 중 하나여야 합니다.")
        if LATEST_STEAM_RANKS["last_updated"] is None:
            return {"status": "pending", "msg": "스팀 랭킹 수집 중입니다."}
        return {
            "status": "success",
            "data": LATEST_STEAM_RANKS[country_upper],
            "updated_at": LATEST_STEAM_RANKS["last_updated"]
        }

    @app.get("/cache/streamer-rank/{platform}")
    def get_cache_streamer_rank(platform: str):
        """백엔드 → 크롤러: 스트리머 랭킹 캐시 조회"""
        platform = platform.lower()
        if platform not in ["chzzk", "twitch"]:
            raise HTTPException(status_code=400, detail="chzzk 또는 twitch 여야 합니다.")
        updated = PLATFORM_RANKINGS.get("last_updated")
        if not updated:
            return {"status": "pending", "msg": "스트리머 데이터 수집 중입니다."}
        return {
            "status": "success",
            "data": PLATFORM_RANKINGS[platform],
            "last_updated": updated
        }

    @app.post("/internal/crawl/{appid}")
    async def request_crawl(appid: int):
        """백엔드 → 크롤러: DB에 없는 게임 수집 요청"""
        if appid in PENDING_QUEUE:
            return {"status": "already_queued", "appid": appid}

        PENDING_QUEUE[appid] = {
            'retry_count': 0,
            'last_attempt': datetime.now()
        }
        print(f"  📥 [크롤 요청] AppID {appid} 대기열 추가")
        return {"status": "queued", "appid": appid}


# =========================================================
# [백엔드 전용] 크롤러 캐시 프록시 헬퍼
# =========================================================
if not IS_CRAWLER:
    import httpx
    import os

    CRAWLER_URL = os.getenv("CRAWLER_URL", "http://cg20-crawler:8099")

    async def get_rates_from_crawler() -> dict | None:
        try:
            async with httpx.AsyncClient() as client:
                res = await client.get(f"{CRAWLER_URL}/cache/rates", timeout=3.0)
                return res.json()
        except Exception as e:
            print(f"  ⚠️ 크롤러 환율 조회 실패: {e}")
            return None

    async def get_steam_ranks_from_crawler(country: str) -> dict | None:
        try:
            async with httpx.AsyncClient() as client:
                res = await client.get(f"{CRAWLER_URL}/cache/steam-ranks/{country}", timeout=3.0)
                return res.json()
        except Exception as e:
            print(f"  ⚠️ 크롤러 스팀 랭킹 조회 실패: {e}")
            return None

    async def get_streamer_rank_from_crawler(platform: str) -> dict | None:
        try:
            async with httpx.AsyncClient() as client:
                res = await client.get(f"{CRAWLER_URL}/cache/streamer-rank/{platform}", timeout=3.0)
                return res.json()
        except Exception as e:
            print(f"  ⚠️ 크롤러 스트리머 랭킹 조회 실패: {e}")
            return None

    async def request_crawl_to_crawler(appid: int):
        try:
            async with httpx.AsyncClient() as client:
                await client.post(f"{CRAWLER_URL}/internal/crawl/{appid}", timeout=3.0)
        except Exception as e:
            print(f"  ⚠️ 크롤러 수집 요청 실패 (AppID {appid}): {e}")


# =========================================================
# [백엔드 전용] API 엔드포인트
# =========================================================
if not IS_CRAWLER:

    @app.get("/rates")
    async def get_current_rates():
        data = await get_rates_from_crawler()
        if not data or data.get("status") == "pending":
            return {"status": "pending", "msg": "환율 정보를 불러오는 중입니다."}
        return data


    @app.get("/steam-ranks/{country}/{start}/{end}")
    async def get_steam_ranks(country: str, start: int, end: int, db: AsyncSession = Depends(get_rdb)):
        if start < 1 or end > 100:
            raise HTTPException(status_code=400, detail="순위는 1~100위까지만 가능합니다.")
        if start > end:
            raise HTTPException(status_code=400, detail="시작 순위가 끝 순위보다 클 수 없습니다.")

        country_upper = country.upper()
        if country_upper not in ["KR", "JP", "US"]:
            raise HTTPException(status_code=400, detail="국가는 KR, JP, US 중 하나여야 합니다.")

        cache = await get_steam_ranks_from_crawler(country_upper)
        if not cache or cache.get("status") == "pending":
            return {"status": "pending", "msg": "스팀 랭킹을 수집 중입니다."}

        rank_ids = cache["data"]
        sliced_ids = rank_ids[start - 1: min(end, len(rank_ids))]
        if not sliced_ids:
            return {"status": "success", "data": [], "updated_at": cache.get("updated_at")}

        query = text("""
            SELECT g.game_id, g.game_name, g.header_image_url, p.price
            FROM games g
            JOIN game_prices p ON g.game_id = p.game_id
            WHERE p.currency = 'KRW' AND g.game_id IN :ids
        """)
        result = await db.execute(query, {"ids": tuple(sliced_ids)})
        game_info_map = {
            row.game_id: {"name": row.game_name, "headerImage": row.header_image_url, "price": row.price}
            for row in result.fetchall()
        }

        final_data = []
        for i, appid in enumerate(sliced_ids):
            info = game_info_map.get(appid, {"name": "정보 수집 중...", "headerImage": None, "price": 0})
            final_data.append({
                "rank": start + i,
                "appid": appid,
                "name": info["name"],
                "headerImage": info["headerImage"],
                "price": info.get("price", 0)
            })

        return {
            "status": "success",
            "country": country_upper,
            "rank_range": f"{start}~{end}",
            "count": len(final_data),
            "data": final_data,
            "updated_at": cache.get("updated_at")
        }


    @app.get("/streamer-rank/{platform}")
    async def get_streamer_rank(platform: str, db: AsyncSession = Depends(get_rdb)):
        platform = platform.lower()
        if platform not in ["chzzk", "twitch"]:
            raise HTTPException(status_code=400, detail="chzzk 또는 twitch 여야 합니다.")

        cache = await get_streamer_rank_from_crawler(platform)
        if not cache or cache.get("status") == "pending":
            return {"status": "pending", "msg": "스트리머 데이터 수집 중입니다."}

        rank_list = cache.get("data", [])
        appids = [s["appid"] for s in rank_list if s.get("appid")]
        if not appids:
            return {"status": "success", "data": [], "last_updated": cache.get("last_updated")}

        query = text("SELECT game_id, game_name, header_image_url FROM games WHERE game_id IN :ids")
        result = await db.execute(query, {"ids": tuple(appids)})
        info_map = {row.game_id: {"name": row.game_name, "headerImage": row.header_image_url} for row in result.fetchall()}

        data = []
        for s in rank_list:
            appid = s.get("appid")
            if not appid:
                continue
            info = info_map.get(appid, {"name": s.get("chzzk_game_name") or s.get("game_name", ""), "headerImage": None})
            data.append({
                "appid": appid,
                "name": info["name"],
                "headerImage": info["headerImage"],
                "viewers": s.get("viewers", 0)
            })

        return {
            "status": "success",
            "platform": platform,
            "data": data,
            "last_updated": cache.get("last_updated")
        }


    @app.get("/steam-game/{appid}")
    async def get_steam_game_info(appid: int):
        async with AsyncSessionLocal() as db:
            query = text("SELECT * FROM games WHERE game_id = :appid")
            result = await db.execute(query, {"appid": appid})
            game_row = result.fetchone()

            if game_row:
                return {"status": "success", "source": "database", "data": dict(game_row._mapping)}

            # DB에 없으면 크롤러에게 수집 요청 후 503 반환
            print(f"🔍 AppID {appid} DB에 없음 → 크롤러에 수집 요청")
            await request_crawl_to_crawler(appid)
            raise HTTPException(
                status_code=503,
                detail="게임 정보를 수집 중입니다. 잠시 후 다시 시도해주세요."
            )


    @app.get("/steam-game/{appid}/price")
    async def get_game_price(appid: int):
        async with AsyncSessionLocal() as db:
            query = text("SELECT currency, price FROM game_prices WHERE game_id = :appid")
            result = await db.execute(query, {"appid": appid})
            prices = {row[0]: float(row[1]) for row in result.fetchall()}

            if not prices:
                await request_crawl_to_crawler(appid)
                raise HTTPException(status_code=503, detail="가격 수집 중입니다. 잠시 후 다시 시도해주세요.")

            return {"status": "success", "appid": appid, "prices": prices}


    @app.get("/steam-game/{appid}/price-detail/{currency}")
    async def get_game_price_detail(appid: int, currency: str):
        currency = currency.upper()
        if currency not in ["KRW", "JPY", "USD"]:
            raise HTTPException(status_code=400, detail="지원하지 않는 통화입니다.")

        async with AsyncSessionLocal() as db:
            query = text("""
                SELECT date, price, regular_price, discount_percent
                FROM game_price_history
                WHERE game_id = :appid AND currency = :currency
                ORDER BY date ASC
            """)
            result = await db.execute(query, {"appid": appid, "currency": currency})
            history = [dict(row._mapping) for row in result.fetchall()]

            if not history:
                await sync_itad_price_history(db, appid)
                result = await db.execute(query, {"appid": appid, "currency": currency})
                history = [dict(row._mapping) for row in result.fetchall()]

            if not history:
                raise HTTPException(status_code=404, detail="가격 히스토리가 없습니다.")

            return {"status": "success", "appid": appid, "currency": currency, "history": history}


    @app.get("/search")
    async def search_games(
        q: str = "",
        genre: str = "전체",
        limit: int = 30,
        db: AsyncSession = Depends(get_rdb)
    ):
        try:
            base_query = (
                "SELECT g.game_id, g.game_name, g.header_image_url, g.game_is_free, "
                "gp.price, "
                "GROUP_CONCAT(gen.genre_name SEPARATOR ', ') as genre_list "
                "FROM games g "
                "LEFT JOIN game_prices gp ON g.game_id = gp.game_id AND gp.currency = 'KRW' "
                "LEFT JOIN game_genres gg ON g.game_id = gg.game_id "
                "LEFT JOIN genres gen ON gg.genre_id = gen.genre_id "
            )
            where_clauses = []
            params = {"limit": limit}

            if q:
                where_clauses.append("(g.game_name LIKE :q OR gen.genre_name LIKE :q)")
                params["q"] = f"%{q}%"
            if genre != "전체":
                where_clauses.append(
                    "g.game_id IN (SELECT game_id FROM game_genres WHERE genre_id = "
                    "(SELECT genre_id FROM genres WHERE genre_name = :genre))"
                )
                params["genre"] = genre

            query_full = base_query
            if where_clauses:
                query_full += " WHERE " + " AND ".join(where_clauses)
            query_full += " GROUP BY g.game_id ORDER BY g.game_id DESC LIMIT :limit"

            result = await db.execute(text(query_full), params)
            data = [{
                "gameId": row.game_id,
                "name": row.game_name,
                "headerImage": row.header_image_url,
                "isFree": bool(row.game_is_free),
                "price": float(row.price) if row.price else 0,
                "genres": row.genre_list.split(", ") if row.genre_list else []
            } for row in result.fetchall()]

            return {"status": "success", "count": len(data), "data": data}
        except Exception as e:
            print(f"❌ /search 에러: {e}")
            raise HTTPException(status_code=500, detail="검색 처리 중 오류가 발생했습니다.")


    @app.get("/search/genres")
    async def get_search_categories(db: AsyncSession = Depends(get_rdb)):
        try:
            result = await db.execute(text("SELECT DISTINCT genre_name FROM genres ORDER BY genre_name ASC"))
            return ["전체"] + [row[0] for row in result.fetchall()]
        except Exception as e:
            print(f"❌ /search/genres 에러: {e}")
            return ["전체"]


# =========================================================
# 진입점
# =========================================================
if __name__ == "__main__":
    import uvicorn

    port = 8099 if IS_CRAWLER else 8000
    print(f"🚀 {'크롤러' if IS_CRAWLER else '백엔드'} 모드로 시작 (포트 {port})")
    uvicorn.run("main:app", host="0.0.0.0", port=port, reload=False)