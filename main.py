# main.py

from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from datetime import datetime

from scrapers import fetch_hana_bank_rates, fetch_all_steam_rankings

# (만약 stream 관련 라우터가 있다면 임포트 유지)
# from stream import chzzk, twitch 
# from services.tasks import run_chzzk_update, run_twitch_update

# =========================================================
# 💾 [핵심] 환율 캐시 (최신 환율을 메모리에 들고 있습니다)
# =========================================================
LATEST_RATES = {
    "standard_usd": 0.0,
    "standard_jpy": 0.0,
    "last_updated": None
}

# [신규] 스팀 랭킹 캐시
LATEST_STEAM_RANKS = {
    "KR": [],
    "JP": [],
    "US": [],
    "last_updated": None
}


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
    # 💡 [핵심] 서버가 딱 켜지자마자 환율을 한 번 긁어옵니다. (안 그러면 첫 5분 동안은 0원이 뜹니다)
    await process_hana_bank()
    await process_steam_rankings()

    scheduler = AsyncIOScheduler()
    scheduler.add_job(process_hana_bank, 'cron', minute='*/5')
    # 💡 스팀 랭킹은 1시간마다 (스팀 랭킹은 자주 안 변합니다)
    scheduler.add_job(process_steam_rankings, 'cron', minute='0')

    # scheduler.add_job(process_chzzk_ranking, 'cron', hour='14,20', minute='0')
    # scheduler.add_job(process_twitch_ranking, 'cron', hour='2,8,14,20', minute='0')

    scheduler.start()
    print("⏰ Background Scheduler Started.")

    yield

    scheduler.shutdown()


app = FastAPI(title="Exchange Rate & StreamRank API", lifespan=lifespan)


# app.include_router(chzzk.router)
# app.include_router(twitch.router)

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