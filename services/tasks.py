import os
from datetime import datetime
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError
from database import AsyncSessionLocal

# 크롤링 및 매칭 함수들 임포트
from services.crowel_chzzk import fetch_chzzk_ranking_official, get_final_match as chzzk_match, clean
from services.crowel_twitch import fetch_twitch_ranking, get_twitch_access_token, get_final_match as twitch_match
from services.steam_api import fetch_full_steam_data, insert_full_game_data


# ==========================================
# 🛠️ [공통] DB에서 스팀 게임 목록 불러와서 매칭 사전 만들기
# ==========================================
async def load_steam_dicts(db):
    """CSV 대신 실제 DB(games 테이블)에서 매칭용 사전을 만듭니다."""
    result = await db.execute(text("SELECT game_id, name FROM games"))

    steam_norm_dict = {}
    steam_raw_dict = {}

    for row in result:
        appid = row.game_id
        raw_name = row.name
        if raw_name:
            norm_name = clean(raw_name)
            steam_norm_dict[norm_name] = appid
            steam_raw_dict[raw_name] = appid

    local_steam_names = list(steam_raw_dict.keys())
    return steam_norm_dict, local_steam_names, steam_raw_dict


# ==========================================
# 🛠️ [공통] 랭킹 저장 및 스팀 정보 Fallback 로직
# ==========================================
async def upsert_streamrank(db, table_name: str, appid: int, num_stream: int, totalviewers: int):
    """랭킹 테이블에 넣고, FK 에러 나면 스팀에서 긁어온 뒤 다시 넣습니다."""
    insert_query = text(f"""
        INSERT INTO {table_name} (game_id, num_stream, totalviewers, last_checked)
        VALUES (:gid, :ns, :tv, :lc)
    """)
    params = {"gid": appid, "ns": num_stream, "tv": totalviewers, "lc": datetime.now()}

    try:
        # 1. 랭킹 저장 시도
        await db.execute(insert_query, params)
        await db.commit()
    except IntegrityError:
        # 2. FK 에러 발생 (games 테이블에 없음!)
        await db.rollback()
        print(f"  ⚠️ AppID {appid} 부모 데이터 없음. 스팀 API 호출 중...")

        full_info = await fetch_full_steam_data(appid)
        if full_info:
            # 3. 스팀 정보(기본/장르/가격) 완전체 적재
            await insert_full_game_data(db, full_info)

            # 4. 랭킹 재저장
            await db.execute(insert_query, params)
            await db.commit()
            print(f"  ✅ [재시도 성공] AppID {appid} 랭킹 저장 완료.")
        else:
            print(f"  ⏭️ [Skip] 스팀 상점 정보가 없어 저장 생략 (AppID {appid})")


# ==========================================
# 🟢 치지직 스케줄러 메인 로직
# ==========================================
async def run_chzzk_update():
    # 백그라운드용 수동 DB 세션 열기
    async with AsyncSessionLocal() as db:
        norm_dict, local_names, raw_dict = await load_steam_dicts(db)
        top_games = fetch_chzzk_ranking_official(limit=100)

        for game in top_games:
            appid, log = chzzk_match(game['chzzk_game_name'], norm_dict, local_names, raw_dict)
            if appid:
                await upsert_streamrank(
                    db, "streamrank_chzzk", appid,
                    game.get('live_count', 0), game.get('viewers', 0)
                )


# ==========================================
# 💜 트위치 스케줄러 메인 로직
# ==========================================
async def run_twitch_update():
    TWITCH_CLIENT_ID = os.getenv("TWITCH_CLIENT_ID")
    TWITCH_CLIENT_SECRET = os.getenv("TWITCH_CLIENT_SECRET")

    token = get_twitch_access_token(TWITCH_CLIENT_ID, TWITCH_CLIENT_SECRET)
    if not token:
        print("❌ 트위치 토큰 발급 실패!")
        return

    async with AsyncSessionLocal() as db:
        norm_dict, local_names, raw_dict = await load_steam_dicts(db)
        twitch_games = fetch_twitch_ranking(TWITCH_CLIENT_ID, token, limit=100)

        for game in twitch_games:
            appid, log = twitch_match(game['game_name'], norm_dict, local_names, raw_dict)
            if appid:
                # 트위치는 API 구조상 현재 라이브 채널 수를 한 번에 안 줘서 0으로 임시 세팅
                await upsert_streamrank(
                    db, "streamrank_twitch", appid,
                    0, game.get('viewers', 0)  # API 응답 구조에 맞게 뷰어 수 매핑 필요
                )