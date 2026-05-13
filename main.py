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
from database import (
    AsyncSessionLocal, connect_to_mongo, close_mongo_connection, get_mongodb, get_rdb,
    connect_to_rdb, connect_to_es, close_es, es
)
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
    await connect_to_rdb()
    await connect_to_es()

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
    await close_es()


# =========================================================
# 백엔드 모드 lifespan
# =========================================================
@asynccontextmanager
async def backend_lifespan(app: FastAPI):
    await connect_to_mongo()
    await connect_to_rdb()
    await connect_to_es()
    yield
    await close_mongo_connection()
    await close_es()


# =========================================================
# FastAPI 앱 생성
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
# [크롤러 전용] 내부 캐시 API
# =========================================================
if IS_CRAWLER:
    @app.get("/cache/rates")
    def get_cache_rates():
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
        platform = platform.lower()
        if platform not in ["chzzk", "twitch"]:
            raise HTTPException(status_code=400, detail="chzzk 또는 twitch 여야 합니다.")
        updated = LIVE_STREAMS.get("last_updated")
        if not updated:
            return {"status": "pending", "msg": "스트리머 데이터 수집 중입니다."}
        return {
            "status": "success",
            "data": LIVE_STREAMS[platform],
            "last_updated": updated
        }

    @app.post("/internal/crawl/{appid}")
    async def request_crawl(appid: int):
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

    async def get_rates_from_crawler():
        try:
            async with httpx.AsyncClient() as client:
                res = await client.get(f"{CRAWLER_URL}/cache/rates", timeout=3.0)
                return res.json()
        except Exception as e:
            print(f"  ⚠️ 크롤러 환율 조회 실패: {e}")
            return None

    async def get_steam_ranks_from_crawler(country: str):
        try:
            async with httpx.AsyncClient() as client:
                res = await client.get(f"{CRAWLER_URL}/cache/steam-ranks/{country}", timeout=3.0)
                return res.json()
        except Exception as e:
            print(f"  ⚠️ 크롤러 스팀 랭킹 조회 실패: {e}")
            return None

    async def get_streamer_rank_from_crawler(platform: str):
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

    # ----------------------------------------------------
    # 환율
    # ----------------------------------------------------
    @app.get("/rates")
    async def get_current_rates():
        data = await get_rates_from_crawler()
        if not data or data.get("status") == "pending":
            return {"status": "pending", "msg": "환율 정보를 불러오는 중입니다."}
        return data


    # ----------------------------------------------------
    # 스팀 랭킹
    # ----------------------------------------------------
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


    # ----------------------------------------------------
    # 스트리머 랭킹 (치지직 / 트위치)
    # ----------------------------------------------------
    async def _build_streamer_rank(platform: str, db: AsyncSession):
        cache = await get_streamer_rank_from_crawler(platform)
        if not cache or cache.get("status") == "pending":
            return {"status": "pending", "msg": "스트리머 데이터 수집 중입니다."}

        data_dict = cache.get("data", {})
        if not data_dict:
            return {"status": "success", "data": [], "last_updated": cache.get("last_updated")}

        sorted_items = sorted(data_dict.items(), key=lambda x: x[1], reverse=True)
        target_ids = [int(appid) for appid, _ in sorted_items]

        query = text("SELECT game_id, game_name, header_image_url FROM games WHERE game_id IN :ids")
        result = await db.execute(query, {"ids": tuple(target_ids)})
        game_info_map = {
            row.game_id: {"name": row.game_name, "headerImage": row.header_image_url}
            for row in result.fetchall()
        }

        final_data = []
        for appid_str, viewers in sorted_items:
            appid = int(appid_str)
            info = game_info_map.get(appid, {"name": "정보 수집 중...", "headerImage": None})
            final_data.append({
                "appid": appid,
                "name": info["name"],
                "headerImage": info["headerImage"],
                "viewers": viewers
            })

        return {
            "status": "success",
            "last_updated": cache.get("last_updated"),
            "data": final_data
        }


    @app.get("/streamer-rank/chzzk")
    async def get_chzzk_streamer_rank(db: AsyncSession = Depends(get_rdb)):
        return await _build_streamer_rank("chzzk", db)


    @app.get("/streamer-rank/twitch")
    async def get_twitch_streamer_rank(db: AsyncSession = Depends(get_rdb)):
        return await _build_streamer_rank("twitch", db)


    # ----------------------------------------------------
    # 게임 상세
    # ----------------------------------------------------
    @app.get("/steam-game/{appid}")
    async def get_steam_game_info(appid: int):
        async with AsyncSessionLocal() as db:
            query = text("SELECT * FROM games WHERE game_id = :appid")
            result = await db.execute(query, {"appid": appid})
            game_row = result.fetchone()

            if game_row:
                return {"status": "success", "source": "database", "data": dict(game_row._mapping)}

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
                raise HTTPException(status_code=404, detail="해당 게임은 가격 추이 정보를 제공하지 않습니다.")

            prices = [float(h['price']) for h in history]
            lowest_price = min(prices)
            latest_price = prices[-1]

            return {
                "status": "success",
                "analysis": {
                    "latest_price": latest_price,
                    "lowest_price": lowest_price,
                    "is_lowest": latest_price <= lowest_price,
                    "buying_advice": "🔥 역대 최저가!" if latest_price <= lowest_price else "⏳ 할인 대기 권장"
                },
                "history": history
            }


    @app.get("/steam-game/{game_id}/news")
    async def get_game_news(game_id: int):
        mongo_db = get_mongodb()
        existing_news = await mongo_db.game_news.find_one({"game_id": game_id})
        if existing_news:
            return {"status": "success", "source": "cache", "data": existing_news['news']}

        print(f"  📡 [News Sync] AppID {game_id} 수집 시작")
        news_items = await fetch_steam_news_only(game_id, count=3)
        await save_game_news_to_mongo(game_id, news_items)
        return {"status": "success", "source": "api", "data": news_items}


    @app.get("/steam-game/{game_id}/reviews")
    async def get_game_reviews(game_id: int):
        mongo_db = get_mongodb()
        existing_reviews = await mongo_db.game_reviews.find_one({"game_id": game_id})
        if existing_reviews:
            return {"status": "success", "source": "cache", "data": existing_reviews['reviews']}

        print(f"  📡 [Review Sync] AppID {game_id} 수집 시작")
        reviews = await fetch_steam_reviews_only(game_id, count=10)
        await save_game_reviews_to_mongo(game_id, reviews)
        return {"status": "success", "source": "api", "data": reviews}


    # ----------------------------------------------------
    # 대시보드 인사이트
    # ----------------------------------------------------
    @app.get("/insights")
    async def get_dashboard_insights(db: AsyncSession = Depends(get_rdb)):
        try:
            cache = await get_steam_ranks_from_crawler("KR")
            if not cache or cache.get("status") == "pending":
                return {"status": "pending", "message": "랭킹 데이터를 수집 중입니다."}

            kr_appids = cache.get("data", [])
            if not kr_appids:
                return {"status": "pending", "message": "랭킹 데이터를 수집 중입니다."}

            appids_tuple = tuple(kr_appids)

            # 장르 점유율
            res_genre = await db.execute(text("""
                SELECT g.genre_name, COUNT(gg.game_id) as cnt
                FROM genres g
                JOIN game_genres gg ON g.genre_id = gg.genre_id
                WHERE gg.game_id IN :appids
                GROUP BY g.genre_name
                ORDER BY cnt DESC LIMIT 6
            """), {"appids": appids_tuple})
            genres = [{"name": row[0], "value": row[1]} for row in res_genre.fetchall()]

            # 무료 vs 유료
            res_free = await db.execute(text("""
                SELECT game_is_free, COUNT(*) as cnt
                FROM games
                WHERE game_id IN :appids
                GROUP BY game_is_free
            """), {"appids": appids_tuple})
            free_paid_data = [{"name": "무료", "value": 0}, {"name": "유료", "value": 0}]
            for row in res_free.fetchall():
                if row[0] == 1:
                    free_paid_data[0]["value"] = row[1]
                else:
                    free_paid_data[1]["value"] = row[1]

            # 최고가 / 최저가
            res_price = await db.execute(text("""
                SELECT g.game_name, gp.price
                FROM games g
                JOIN game_prices gp ON g.game_id = gp.game_id
                WHERE g.game_id IN :appids
                  AND gp.currency = 'KRW'
                  AND g.game_is_free = 0
                ORDER BY gp.price DESC
            """), {"appids": appids_tuple})
            prices = res_price.fetchall()
            highest = {"name": prices[0][0], "price": float(prices[0][1])} if prices else None
            lowest = {"name": prices[-1][0], "price": float(prices[-1][1])} if prices else None

            # 지원 언어
            res_lang = await db.execute(text("""
                SELECT l.language_name, COUNT(gl.game_id) as cnt
                FROM languages l
                JOIN game_languages gl ON l.language_id = gl.language_id
                WHERE gl.game_id IN :appids
                GROUP BY l.language_name
                ORDER BY cnt DESC LIMIT 5
            """), {"appids": appids_tuple})
            languages = [{"name": row[0], "value": row[1]} for row in res_lang.fetchall()]

            # OS
            res_os = await db.execute(text("""
                SELECT SUM(os_windows), SUM(os_mac), SUM(os_linux)
                FROM games
                WHERE game_id IN :appids
            """), {"appids": appids_tuple})
            os_row = res_os.fetchone()
            os_support = [
                {"name": "Windows", "value": int(os_row[0] or 0)},
                {"name": "Mac",     "value": int(os_row[1] or 0)},
                {"name": "Linux",   "value": int(os_row[2] or 0)},
            ]

            # 출시 연도
            res_year = await db.execute(text("""
                SELECT YEAR(game_releaseDate) as yr, COUNT(*) as cnt
                FROM games
                WHERE game_id IN :appids
                  AND game_releaseDate IS NOT NULL
                  AND YEAR(game_releaseDate) > 2010
                GROUP BY yr
                ORDER BY yr ASC
            """), {"appids": appids_tuple})
            years = [{"name": str(row[0]), "value": row[1]} for row in res_year.fetchall()]

            return {
                "status": "success",
                "data": {
                    "genreShare": genres,
                    "freeVsPaid": free_paid_data,
                    "priceExtremes": {"highest": highest, "lowest": lowest},
                    "languages": languages,
                    "osSupport": os_support,
                    "releaseYears": years
                }
            }
        except Exception as e:
            return {"status": "error", "message": str(e)}


    @app.get("/insights/discount-frequency")
    async def get_discount_frequency(db: AsyncSession = Depends(get_rdb)):
        try:
            query_string = (
                "SELECT YEAR" + "(g.game_releaseDate) as release_year, "
                "COUNT" + "(DISTINCT g.game_id) as game_count, "
                "COALESCE" + "(" + "SUM" + "(CASE WHEN h.discount_percent > 0 THEN 1.0 ELSE 0.0 END) / "
                "COUNT" + "(h.game_id) * 100, 0) as avg_frequency "
                "FROM games g JOIN game_price_history h ON g.game_id = h.game_id "
                "WHERE g.game_releaseDate IS NOT NULL AND YEAR" + "(g.game_releaseDate) BETWEEN 2010 AND YEAR" + "(CURDATE()) "
                "GROUP BY YEAR" + "(g.game_releaseDate) "
                "ORDER BY release_year ASC"
            )
            result = await db.execute(text(query_string))
            data = [{
                "year":        int(row.release_year),
                "avgDiscount": round(float(row.avg_frequency), 1),
                "gameCount":   int(row.game_count)
            } for row in result.fetchall()]
            return data
        except Exception as e:
            print(f"❌ /insights/discount-frequency 에러: {e}")
            raise HTTPException(status_code=500, detail="데이터 통계 분석 중 오류가 발생했습니다.")


    # ----------------------------------------------------
    # 게임 목록
    # ----------------------------------------------------
    @app.get("/games")
    async def get_games_list(limit: int = 200, db: AsyncSession = Depends(get_rdb)):
        try:
            query_string = (
                "SELECT g.game_id, g.game_name, "
                "GROUP_CONCAT" + "(gen.genre_name SEPARATOR ',') as genres "
                "FROM games g "
                "LEFT JOIN game_genres gg ON g.game_id = gg.game_id "
                "LEFT JOIN genres gen ON gg.genre_id = gen.genre_id "
                "GROUP BY g.game_id, g.game_name "
                "ORDER BY g.game_id DESC "
                "LIMIT :limit"
            )
            result = await db.execute(text(query_string), {"limit": limit})
            data = [{
                "gameId": int(row.game_id),
                "name":   str(row.game_name),
                "genres": row.genres.split(",") if row.genres else []
            } for row in result.fetchall()]
            return data
        except Exception as e:
            print(f"❌ /games 에러: {e}")
            raise HTTPException(status_code=500, detail="게임 목록을 불러오지 못했습니다.")


    # ----------------------------------------------------
    # 인사이트 - 장르 트렌드
    # ----------------------------------------------------
    @app.get("/insight/genre-trend")
    async def get_genre_trend(db: AsyncSession = Depends(get_rdb)):
        try:
            query_string = (
                "SELECT YEAR" + "(g.game_releaseDate) as yr, "
                "gen.genre_name, "
                "COUNT" + "(DISTINCT g.game_id) as cnt "
                "FROM games g "
                "JOIN game_genres gg ON g.game_id = gg.game_id "
                "JOIN genres gen ON gg.genre_id = gen.genre_id "
                "WHERE g.game_releaseDate IS NOT NULL "
                "AND YEAR" + "(g.game_releaseDate) BETWEEN 2018 AND YEAR" + "(CURDATE()) "
                "GROUP BY yr, gen.genre_name"
            )
            result = await db.execute(text(query_string))
            rows = result.fetchall()

            years = sorted(list(set(int(r.yr) for r in rows)))

            genre_totals = {}
            for r in rows:
                genre_totals[r.genre_name] = genre_totals.get(r.genre_name, 0) + r.cnt
            top_genres = sorted(genre_totals, key=genre_totals.get, reverse=True)[:5]

            matrix = []
            for genre in top_genres:
                genre_row = []
                for year in years:
                    year_total = sum(r.cnt for r in rows if int(r.yr) == year)
                    g_count = sum(r.cnt for r in rows if int(r.yr) == year and r.genre_name == genre)
                    pct = round((g_count / year_total) * 100, 1) if year_total > 0 else 0
                    genre_row.append(pct)
                matrix.append(genre_row)

            return {"years": years, "genres": top_genres, "matrix": matrix}
        except Exception as e:
            print(f"❌ /insight/genre-trend 에러: {e}")
            raise HTTPException(status_code=500, detail="장르 트렌드 오류")


    # ----------------------------------------------------
    # 인사이트 - 가짜 할인 의심
    # ----------------------------------------------------
    @app.get("/insight/fake-discount-ranking")
    async def get_fake_discount_ranking(db: AsyncSession = Depends(get_rdb)):
        try:
            query_string = (
                "SELECT g.game_id, g.game_name, "
                "MAX" + "(h.regular_price) as max_reg, "
                "MIN" + "(h.regular_price) as min_reg, "
                "COUNT" + "(DISTINCT h.date) as change_cnt, "
                "MAX" + "(h.discount_percent) as max_discount "
                "FROM games g "
                "JOIN game_price_history h ON g.game_id = h.game_id "
                "WHERE h.currency = 'KRW' "
                "GROUP BY g.game_id, g.game_name "
                "HAVING max_reg > min_reg "
                "ORDER BY change_cnt DESC "
                "LIMIT 5"
            )
            result = await db.execute(text(query_string))

            data = []
            for row in result.fetchall():
                max_reg    = float(row.max_reg)
                min_reg    = float(row.min_reg)
                change_cnt = int(row.change_cnt)
                max_disc   = float(row.max_discount) if row.max_discount else 0

                price_hike_pct = round((max_reg - min_reg) / min_reg * 100, 1) if min_reg > 0 else 0
                score = min(100, int(price_hike_pct) + change_cnt * 5)

                if   score >= 80: grade = "매우의심"
                elif score >= 50: grade = "약간의심"
                elif score >= 30: grade = "주의"
                else:             grade = "정상"

                reasons = []
                if price_hike_pct >= 50:
                    reasons.append(f"정가가 최저 ₩{int(min_reg):,} → 최고 ₩{int(max_reg):,}으로 {price_hike_pct}% 인상된 이력")
                elif price_hike_pct >= 20:
                    reasons.append(f"정가 변동폭 {price_hike_pct}% (₩{int(min_reg):,} → ₩{int(max_reg):,})")
                else:
                    reasons.append(f"정가 소폭 변동 {price_hike_pct}%")

                if change_cnt >= 100:
                    reasons.append(f"가격 변동이 {change_cnt}회로 매우 잦음")
                elif change_cnt >= 30:
                    reasons.append(f"가격 변동 {change_cnt}회")

                if max_disc >= 90:
                    reasons.append(f"최대 {int(max_disc)}% 할인 이력 (상시 할인 의심)")
                elif max_disc >= 70:
                    reasons.append(f"최대 {int(max_disc)}% 할인 이력")

                data.append({
                    "gameId":       int(row.game_id),
                    "name":         str(row.game_name),
                    "score":        score,
                    "grade":        grade,
                    "reason":       " / ".join(reasons),
                    "maxRegPrice":  int(max_reg),
                    "minRegPrice":  int(min_reg),
                    "priceHikePct": price_hike_pct,
                    "changeCnt":    change_cnt,
                    "maxDiscount":  int(max_disc),
                })

            return sorted(data, key=lambda x: x["score"], reverse=True)
        except Exception as e:
            print(f"❌ /insight/fake-discount-ranking 에러: {e}")
            raise HTTPException(status_code=500, detail="가짜 할인 분석 오류")


    # ----------------------------------------------------
    # 인사이트 - 국가별 가격
    # ----------------------------------------------------
    @app.get("/insight/country-price/{game_id}")
    async def get_country_price(game_id: int, db: AsyncSession = Depends(get_rdb)):
        try:
            query_string = (
                "SELECT g.game_name, p.currency, p.price, g.header_image_url "
                "FROM games g "
                "JOIN game_prices p ON g.game_id = p.game_id "
                "WHERE g.game_id = :gid"
            )
            result = await db.execute(text(query_string), {"gid": game_id})
            rows = result.fetchall()

            if not rows:
                raise HTTPException(status_code=404, detail="가격 정보가 없습니다.")

            prices = {r.currency: float(r.price) for r in rows}
            return {
                "gameId": game_id,
                "name": rows[0].game_name,
                "prices": prices,
                "headerImage": rows[0].header_image_url
            }
        except Exception as e:
            print(f"❌ /insight/country-price 에러: {e}")
            raise HTTPException(status_code=500, detail="가격 비교 오류")


    # ----------------------------------------------------
    # 인사이트 - 리뷰 감성분석
    # ----------------------------------------------------
    @app.get("/insight/review-sentiment/{game_id}")
    async def get_review_sentiment(game_id: int):
        try:
            mongo_db = get_mongodb()
            doc = await mongo_db.game_reviews.find_one({"game_id": game_id})

            if not doc or not doc.get("reviews"):
                raise HTTPException(status_code=404, detail="리뷰 데이터가 없습니다.")

            reviews   = doc["reviews"]
            total     = len(reviews)
            pos_count = sum(1 for r in reviews if r.get("is_positive"))
            neg_count = total - pos_count

            words = []
            for r in reviews:
                content = str(r.get("content", ""))
                clean_text = re.sub(r'[^가-힣a-zA-Z\s]', '', content)
                words.extend([w for w in clean_text.split() if len(w) >= 2])

            word_counts = Counter(words).most_common(8)

            keywords = []
            for word, count in word_counts:
                weight = min(5, max(1, count // 2))
                is_pos = True if any(c in word for c in ['재밌', '갓', '좋', '최고', '추천', 'fun', 'good']) else False
                keywords.append({"text": word, "weight": weight, "isPos": is_pos})

            return {
                "positive": int((pos_count / total) * 100) if total > 0 else 0,
                "negative": int((neg_count / total) * 100) if total > 0 else 0,
                "totalReviews": total,
                "keywords": keywords
            }
        except Exception as e:
            print(f"❌ /insight/review-sentiment 에러: {e}")
            raise HTTPException(status_code=500, detail="리뷰 감성분석 오류")


    # ----------------------------------------------------
    # 통합 검색 (Elasticsearch)
    # ----------------------------------------------------
    @app.get("/search")
    async def search_games(q: str = "", genre: str = "전체", limit: int = 30):
        try:
            must = []
            if q:
                must.append({
                    "multi_match": {
                        "query": q,
                        "fields": ["game_name^3", "developers^2", "publishers^2"],
                        "fuzziness": "AUTO"
                    }
                })
            if genre != "전체":
                must.append({"term": {"genres": genre}})

            body = {
                "query": {"bool": {"must": must}} if must else {"match_all": {}},
                "size": limit
            }
            result = await es.search(index="games", body=body)
            data = [hit["_source"] for hit in result["hits"]["hits"]]
            return {"status": "success", "count": len(data), "data": data}
        except Exception as e:
            print(f"❌ [ES 검색 에러] {e}")
            raise HTTPException(status_code=500, detail="검색 오류")


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