# main.py
import asyncio
#import scheduler

from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException, Depends
from fastapi.middleware.cors import CORSMiddleware
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from datetime import datetime

from sqlalchemy.ext.asyncio import AsyncSession

from scrapers import fetch_hana_bank_rates, fetch_all_steam_rankings
from services.itad_api import sync_itad_price_history
from services.steam_api import fetch_full_steam_data, insert_full_game_data, fetch_steam_news_only, \
    fetch_steam_reviews_only, save_game_reviews_to_mongo, save_game_news_to_mongo
from database import AsyncSessionLocal, connect_to_mongo, close_mongo_connection, get_mongodb, get_rdb

from sqlalchemy import text

from services.stream_tasks import update_chzzk_rank, update_twitch_rank

# (만약 stream 관련 라우터가 있다면 임포트 유지)
# from stream import chzzk, twitch
# from services.tasks import run_chzzk_update, run_twitch_update

# =========================================================
# 💾 [핵심] 환율 캐시 (최신 환율을 메모리에 들고 있습니다)
# =========================================================
# LATEST_RATES = {
#     "standard_usd": 0.0,
#     "standard_jpy": 0.0,
#     "last_updated": None
# }
#
# # [신규] 스팀 랭킹 캐시
# LATEST_STEAM_RANKS = {
#     "KR": [],
#     "JP": [],
#     "US": [],
#     "last_updated": None
# }
#
# # Steam API Fetch 대기열
# PENDING_QUEUE = {}
from store import LATEST_RATES, LATEST_STEAM_RANKS, PENDING_QUEUE, PLATFORM_RANKINGS

scheduler = AsyncIOScheduler()

# ---------------------------------------------------------
# 스크래핑 작업 (매 5분)
# ---------------------------------------------------------
async def process_hana_bank():
    print(f"\n[{datetime.now()}] 💰 --- Cron Job: Exchange Rate Update Started ---")

    hana_data = await fetch_hana_bank_rates()

    if hana_data:
        # 💡 긁어온 데이터를 전역 캐시에 업데이트!
        LATEST_RATES["standard_usd"] = hana_data.get("standard_usd", 0.0)
        LATEST_RATES["standard_jpy"] = hana_data.get("standard_jpy", 0.0)
        LATEST_RATES["last_updated"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        print(f"  ✅ [하나은행 캐시 업데이트] USD: {LATEST_RATES['standard_usd']}, JPY: {LATEST_RATES['standard_jpy']}")
    else:
        print("  ❌ [하나은행] 데이터 수집 실패")

    print(f"[{datetime.now()}] 💰 --- Cron Job: Exchange Rate Update Finished ---")


# ---------------------------------------------------------
# [신규] 스팀 랭킹 스크래핑 작업 (매 1시간)
# ---------------------------------------------------------
async def process_steam_rankings():
    steam_data = await fetch_all_steam_rankings()

    if steam_data:
        # 전역 캐시 업데이트
        LATEST_STEAM_RANKS["KR"] = steam_data["KR"]
        LATEST_STEAM_RANKS["JP"] = steam_data["JP"]
        LATEST_STEAM_RANKS["US"] = steam_data["US"]
        LATEST_STEAM_RANKS["last_updated"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f"  ✅ [스팀 캐시 업데이트] 밴 게임 {steam_data['banned_count']}개 제외 완료")

# (치지직/트위치 스케줄러 로직은 그대로 유지...)

# ---------------------------------------------------------
# FastAPI 수명 주기
# ---------------------------------------------------------
@asynccontextmanager
async def lifespan(app: FastAPI):
    await connect_to_mongo()

    # 1. 환율 및 스팀 랭킹 초기 수집
    await process_hana_bank()
    await process_steam_rankings()

    # 2. 스케줄러 세팅
    scheduler = AsyncIOScheduler()
    scheduler.add_job(process_hana_bank, 'cron', minute='*/5')
    scheduler.add_job(process_steam_rankings, 'cron', minute='0')

    # 💡 [핵심] 치지직: 매일 14:00, 20:00 (오후 2시, 8시)
    scheduler.add_job(update_chzzk_rank, 'cron', hour='14,20', minute=0)

    # 💡 [핵심] 트위치: 매일 02:00, 08:00, 14:00, 20:00 (오전/오후 2시, 8시)
    scheduler.add_job(update_twitch_rank, 'cron', hour='2,8,14,20', minute=0)

    scheduler.start()

    # 💡 [핵심] 서버 시작 직후 즉시 수집 (Background Task)
    asyncio.create_task(update_chzzk_rank())
    asyncio.create_task(update_twitch_rank())

    yield
    scheduler.shutdown()
    await close_mongo_connection()


app = FastAPI(title="Exchange Rate & StreamRank API", lifespan=lifespan)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"]
)


# app.include_router(chzzk.router)
# app.include_router(twitch.router)

async def process_memory_queue():
    """5분마다 실행되며 파이썬 메모리(PENDING_QUEUE)에 쌓인 게임을 재수집합니다."""

    if not PENDING_QUEUE:
        return  # 큐가 비어있으면 쿨하게 패스

    # 💡 딕셔너리에서 재시도 5회 미만인 게임들을 '가장 오래된 순'으로 5개만 뽑아냅니다.
    target_items = sorted(
        [(k, v) for k, v in PENDING_QUEUE.items() if v['retry_count'] < 5],
        key=lambda x: x[1]['last_attempt']
    )[:5]

    if not target_items:
        return

    print(f"\n[{datetime.now()}] 🛒 --- 메모리 대기열 처리 시작 ({len(target_items)}개) ---")

    async with AsyncSessionLocal() as db:
        for appid, meta in target_items:
            full_info = await fetch_full_steam_data(appid)

            if full_info:
                # 성공! DB 적재 후 메모리 큐에서 삭제
                await insert_full_game_data(db, full_info)
                del PENDING_QUEUE[appid]
                print(f"  ✅ [메모리 큐 정리 완료] AppID {appid} 저장 성공!")
            else:
                # 또 실패... 카운트 1 올리고 시간 갱신
                PENDING_QUEUE[appid]['retry_count'] += 1
                PENDING_QUEUE[appid]['last_attempt'] = datetime.now()
                print(f"  ⚠️ [메모리 큐 지연] AppID {appid} 실패 (재시도: {PENDING_QUEUE[appid]['retry_count']}회)")

            # API 제한 방어용 2초 휴식
            await asyncio.sleep(2.0)

        print(f"[{datetime.now()}] 🛒 --- 메모리 대기열 처리 완료 ---")

scheduler.add_job(
    process_memory_queue,
    trigger='interval',
    minutes=5,
    id='process_memory_queue'
)

@app.get("/")
def read_root():
    return {"msg": "StreamRank Server is running!🚀"}


# =========================================================
# 🌐 [신규 API] /rates 접속 시 환율 보여주기!
# =========================================================
@app.get("/rates")
def get_current_rates():
    """메모리에 캐싱된 가장 최근의 환율 정보를 반환합니다."""
    # 만약 서버가 켜진 직후라 데이터가 없다면 경고 메시지 반환
    if LATEST_RATES["last_updated"] is None:
        return {
            "status": "pending",
            "msg": "환율 정보를 불러오는 중입니다. 잠시 후 다시 시도해주세요."
        }

    return {
        "status": "success",
        "data": {
            "usd": LATEST_RATES["standard_usd"],
            "jpy": LATEST_RATES["standard_jpy"]
        },
        "updated_at": LATEST_RATES["last_updated"]
    }


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("main:app", host="127.0.0.1", port=8000, reload=True)


# =========================================================
# 🌐 [수정된 API] /steam-ranks/{country}/{start}/{end}
# =========================================================
@app.get("/steam-ranks/{country}/{start}/{end}")
def get_steam_ranks(country: str, start: int, end: int):
    """
    국가별 스팀 랭킹을 지정된 순위 범위만큼 반환합니다.
    - country: KR, JP, US 중 하나
    - start: 시작 순위 (1 이상 100 이하)
    - end: 끝 순위 (1 이상 100 이하, start 이상)
    """
    # 1. 캐시 준비 확인
    if LATEST_STEAM_RANKS["last_updated"] is None:
        return {"status": "pending", "msg": "스팀 랭킹을 수집 중입니다."}

    # 2. 국가 파라미터 대문자 변환 및 유효성 검사
    country_upper = country.upper()
    if country_upper not in ["KR", "JP", "US"]:
        raise HTTPException(status_code=400, detail="국가는 KR, JP, US 중 하나여야 합니다.")

    # 3. 순위 범위 예외 처리 (대표님 기획 반영!)
    if start < 1 or end > 100:
        raise HTTPException(status_code=400, detail=f"순위는 1위부터 100위까지만 조회 가능합니다. (요청: {start}~{end})")

    if start > end:
        raise HTTPException(status_code=400, detail="시작 순위는 끝 순위보다 클 수 없습니다.")

    # 4. 데이터 슬라이싱 (파이썬 인덱스는 0부터 시작하므로 -1 처리)
    # 예: 1위 ~ 20위 요청 -> 인덱스 0 ~ 19 슬라이싱
    target_rank_list = LATEST_STEAM_RANKS[country_upper]

    # 만약 수집된 게임이 100개가 안 될 경우를 대비한 안전장치
    actual_end = min(end, len(target_rank_list))

    sliced_data = target_rank_list[start - 1: actual_end]

    return {
        "status": "success",
        "country": country_upper,
        "rank_range": f"{start}~{end}",
        "count": len(sliced_data),
        "data": sliced_data,
        "updated_at": LATEST_STEAM_RANKS["last_updated"]
    }


@app.get("/steam-game/{appid}")
async def get_steam_game_info(appid: int):
    """
    특정 스팀 게임의 상세 정보를 조회합니다.
    1. DB 확인 -> 2. 없으면 실시간 스팀 API 호출 -> 3. 지역락/에러 처리 및 DB 적재
    """
    async with AsyncSessionLocal() as db:
        # [Step 1] DB에 이미 정보가 있는지 확인
        query = text("SELECT * FROM games WHERE game_id = :appid")
        result = await db.execute(query, {"appid": appid})
        game_row = result.fetchone()

        if game_row:
            # DB에 있으면 바로 반환 (딕셔너리 형태로 변환)
            return {
                "status": "success",
                "source": "database",
                "data": dict(game_row._mapping)
            }

        # [Step 2] DB에 정보가 없다면? 스팀 API 실시간 호출 (Lazy Loading)
        print(f"🔍 [API 요청] DB에 정보 없음. AppID {appid} 실시간 수집 시작...")
        full_info = await fetch_full_steam_data(appid)

        # [Step 3] 대표님 요청사항: 지역락(BANNED) 처리
        if full_info == "BANNED":
            return {
                "status": "country_unavailable",
                "message": "한국 스토어에서 지역락(구매 제한) 또는 삭제된 게임입니다.",
                "appid": appid
            }

        # [Step 4] 스팀 API 호출 제한 또는 통신 에러 처리
        elif full_info == "RETRY" or full_info is None:
            # 프론트엔드에게는 HTTP 503(Service Unavailable) 또는 적절한 상태 코드로 응답
            raise HTTPException(
                status_code=503,
                detail="스팀 서버와 통신이 지연되고 있습니다. 잠시 후 다시 시도해주세요."
            )

        # [Step 5] 수집 성공! DB에 즉시 적재하고 프론트엔드에 반환
        try:
            await insert_full_game_data(db, full_info)
            print(f"✨ [API 실시간 적재] AppID {appid} 저장 완료.")

            return {
                "status": "success",
                "source": "live_fetch",
                "data": full_info
            }
        except Exception as e:
            print(f"❌ [API 적재 에러] AppID {appid} 저장 중 오류: {e}")
            raise HTTPException(status_code=500, detail="데이터베이스 저장 중 오류가 발생했습니다.")


# ==========================================
# 💰 1. 현재 가격 조회 API (3개국 통화)
# ==========================================
@app.get("/steam-game/{appid}/price")
async def get_game_price(appid: int):
    """
    특정 게임의 현재 KRW, JPY, USD 가격을 반환합니다.
    """
    async with AsyncSessionLocal() as db:
        # [Step 1] DB에서 현재 가격 조회
        query = text("SELECT currency, price FROM game_prices WHERE game_id = :appid")
        result = await db.execute(query, {"appid": appid})
        prices = {row[0]: float(row[1]) for row in result.fetchall()}

        # [Step 2] 데이터가 없다면? 실시간 수집 시도
        if not prices:
            print(f"🔍 [Price API] 정보 없음. AppID {appid} 가격 실시간 수집 시작...")
            full_info = await fetch_full_steam_data(appid)

            if isinstance(full_info, dict):
                # 수집 성공 시 DB 저장 후 결과 구성
                await insert_full_game_data(db, full_info)
                prices = {cc: float(p) for cc, p in full_info['prices'].items()}
            elif full_info == "BANNED":
                return {"status": "country_unavailable", "message": "지역락 게임은 가격 조회가 불가능합니다."}
            else:
                raise HTTPException(status_code=503, detail="스팀 API 통신 지연. 잠시 후 시도해주세요.")

        return {
            "status": "success",
            "appid": appid,
            "prices": prices
        }


# ==========================================
# 📈 2. 가격 추이 분석 API (역대 최저가 비교)
# ==========================================
@app.get("/steam-game/{appid}/price-detail/{currency}")
async def get_game_price_detail(appid: int, currency: str):
    currency = currency.upper()
    if currency not in ["KRW", "JPY", "USD"]:
        raise HTTPException(status_code=400, detail="지원하지 않는 통화입니다.")

    async with AsyncSessionLocal() as db:
        # [Step 1] DB에서 히스토리 조회
        query = text("""
                     SELECT date, price, regular_price, discount_percent
                     FROM game_price_history
                     WHERE game_id = :appid AND currency = :currency
                     ORDER BY date ASC
                     """)
        result = await db.execute(query, {"appid": appid, "currency": currency})
        history = [dict(row._mapping) for row in result.fetchall()]

        # [Step 2] 데이터가 없다면? ITAD API 동기화 실행
        if not history:
            print(f"📊 [History API] 정보 없음. AppID {appid} ITAD 히스토리 수집 시작...")
            # ITAD 데이터 수집 및 DB 적재 (itad_api.py 함수 호출)
            await sync_itad_price_history(db, appid)

            # 적재 후 다시 조회
            result = await db.execute(query, {"appid": appid, "currency": currency})
            history = [dict(row._mapping) for row in result.fetchall()]

        # [Step 3] 여전히 없거나 수집 실패 시 처리
        if not history:
            raise HTTPException(status_code=404, detail="해당 게임은 가격 추이 정보를 제공하지 않습니다.")

        # [Step 4] 분석 로직 (최저가 비교 등)
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
    # 1. MongoDB에서 캐시 확인
    existing_news = await mongo_db.game_news.find_one({"game_id": game_id})
    if existing_news:
        return {"status": "success", "source": "cache", "data": existing_news['news']}

    # 2. 스팀 API 수집
    print(f"  📡 [News Sync] AppID {game_id} 수집 시작")
    news_items = await fetch_steam_news_only(game_id, count=3)

    # 3. 💡 분리해둔 함수로 깔끔하게 저장!
    await save_game_news_to_mongo(game_id, news_items)

    return {"status": "success", "source": "api", "data": news_items}


@app.get("/steam-game/{game_id}/reviews")
async def get_game_reviews(game_id: int):
    mongo_db = get_mongodb()
    # 1. MongoDB에서 캐시 확인
    existing_reviews = await mongo_db.game_reviews.find_one({"game_id": game_id})
    if existing_reviews:
        return {"status": "success", "source": "cache", "data": existing_reviews['reviews']}

    # 2. 스팀 API 수집
    print(f"  📡 [Review Sync] AppID {game_id} 수집 시작")
    reviews = await fetch_steam_reviews_only(game_id, count=10)

    # 3. 💡 분리해둔 함수로 깔끔하게 저장!
    await save_game_reviews_to_mongo(game_id, reviews)

    return {"status": "success", "source": "api", "data": reviews}

@app.get("/streamer-rank/chzzk")
async def get_chzzk_streamer_rank():
    if LATEST_STEAM_RANKS["last_updated"] is None:
        return {
            "status": "pending",
            "msg": "랭크 수집 중이에요! 네르지 마시고, 조금만 더 기다려 주세요! 🏃‍♂️💨"
        }
    # 💡 이미 stream_tasks에서 가공된 랭킹 데이터를 그대로 반환
    return {
        "status": "success",
        "last_updated": PLATFORM_RANKINGS["last_updated"],
        "data": PLATFORM_RANKINGS["chzzk"]
    }

@app.get("/streamer-rank/twitch")
async def get_twitch_streamer_rank():
    if LATEST_STEAM_RANKS["last_updated"] is None:
        return {
            "status": "pending",
            "msg": "랭크 수집 중이에요! 네르지 마시고, 조금만 더 기다려 주세요! 🏃‍♂️💨"
        }
    return {
        "status": "success",
        "last_updated": PLATFORM_RANKINGS["last_updated"],
        "data": PLATFORM_RANKINGS["twitch"]
    }

@app.get("/insights")
async def get_dashboard_insights(db: AsyncSession = Depends(get_rdb)):
    try:
        # 💡 1. 캐시에서 한국(KR) 랭킹 AppID 리스트 가져오기
        kr_appids = LATEST_STEAM_RANKS.get("KR", [])

        # 만약 아직 랭킹 수집이 안 되었다면 빈 값 반환
        if not kr_appids:
            return {"status": "pending", "message": "랭킹 데이터를 수집 중입니다."}

        # SQL IN 절에 넣기 위해 튜플로 변환 (요소가 1개일 때를 대비해 문자열 처리 조심해야 하지만, 튜플 전달이 가장 안전함)
        appids_tuple = tuple(kr_appids)

        # 📊 1. 장르별 점유율 (Top 6)
        res_genre = await db.execute(text("""
                                          SELECT g.genre_name, COUNT(gg.game_id) as cnt
                                          FROM genres g
                                                   JOIN game_genres gg ON g.genre_id = gg.genre_id
                                          WHERE gg.game_id IN :appids
                                          GROUP BY g.genre_name
                                          ORDER BY cnt DESC LIMIT 6
                                          """), {"appids": appids_tuple})
        genres = [{"name": row[0], "value": row[1]} for row in res_genre.fetchall()]

        # 🍩 2. 무료 vs 유료 비율
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

        # 💰 3. 최고가 / 최저가 게임 (무료게임 제외)
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

        # 🌐 4. 지원 언어 TOP 5
        res_lang = await db.execute(text("""
                                         SELECT l.language_name, COUNT(gl.game_id) as cnt
                                         FROM languages l
                                                  JOIN game_languages gl ON l.language_id = gl.language_id
                                         WHERE gl.game_id IN :appids
                                         GROUP BY l.language_name
                                         ORDER BY cnt DESC LIMIT 5
                                         """), {"appids": appids_tuple})
        languages = [{"name": row[0], "value": row[1]} for row in res_lang.fetchall()]

        # 💻 5. 운영체제 지원 비율
        res_os = await db.execute(text("""
                                       SELECT SUM(os_windows), SUM(os_mac), SUM(os_linux)
                                       FROM games
                                       WHERE game_id IN :appids
                                       """), {"appids": appids_tuple})
        os_row = res_os.fetchone()
        os_support = [
            {"name": "Windows", "value": int(os_row[0] or 0)},
            {"name": "Mac", "value": int(os_row[1] or 0)},
            {"name": "Linux", "value": int(os_row[2] or 0)},
        ]

        # 📅 6. 출시 연도별 트렌드 (최근 10년 위주)
        # 📅 6. 출시 연도별 트렌드 (최근 10년 위주)
        res_year = await db.execute(text("""
                                         SELECT YEAR (game_releaseDate) as yr, COUNT (*) as cnt
                                         FROM games
                                         WHERE game_id IN :appids
                                           AND game_releaseDate IS NOT NULL
                                           AND YEAR (game_releaseDate)
                                             > 2010
                                         GROUP BY yr
                                         ORDER BY yr ASC
                                         """), {"appids": appids_tuple})
        years = [{"name": str(row[0]), "value": row[1]} for row in res_year.fetchall()]

        # 🎁 프론트엔드에 한 번에 전달
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