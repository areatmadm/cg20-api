# services/stream_tasks.py
import asyncio
from sqlalchemy import text
from datetime import datetime
from store import LIVE_STREAMS, PLATFORM_RANKINGS
from database import AsyncSessionLocal

# 💡 대표님의 크롤러 (이제 에러 없이 루트에서 가져옵니다)
from .crowel_chzzk import fetch_chzzk_ranking_official, get_final_match as get_match_ch, clean
from .crowel_twitch import fetch_twitch_ranking, get_twitch_access_token, get_final_match as get_match_tw

# 매칭을 위한 3대장 사전
STEAM_NORM_DICT = {}  # 제초된 이름 -> AppID
STEAM_RAW_DICT = {}  # 원본 이름 -> AppID
LOCAL_STEAM_NAMES = []  # 유사도 검사용 리스트


async def load_steam_dict():
    """MariaDB games 테이블에서 매칭 사전을 로드합니다."""
    global STEAM_NORM_DICT, STEAM_RAW_DICT, LOCAL_STEAM_NAMES
    if STEAM_NORM_DICT: return

    try:
        async with AsyncSessionLocal() as db:
            result = await db.execute(text("SELECT game_id, game_name FROM games"))
            rows = result.fetchall()
            for row in rows:
                appid, name = int(row[0]), str(row[1]).strip()
                if appid and name:
                    STEAM_NORM_DICT[clean(name)] = appid
                    STEAM_RAW_DICT[name] = appid
            LOCAL_STEAM_NAMES = list(STEAM_RAW_DICT.keys())
        print(f"✅ MariaDB 매칭 사전 로드 완료 (총 {len(STEAM_NORM_DICT)}개)")
    except Exception as e:
        print(f"❌ 매칭 사전 로드 실패: {e}")


async def update_chzzk_rank():
    """치지직 실시간 인기 게임 수집 (매일 14시, 20시)"""
    print(f"[{datetime.now()}] 🟢 [Chzzk] 정기 수집 시작...")
    await load_steam_dict()
    try:
        ch_list = await asyncio.to_thread(fetch_chzzk_ranking_official, limit=100)
        new_ch_sum, new_ch_rank = {}, []

        for s in ch_list:
            g_name = s.get('game_name')
            viewers = int(s.get('viewers', 0))

            # [엄격한 필터링] 스팀 AppID 확인된 게임만 통과
            appid, _ = await asyncio.to_thread(get_match_ch, g_name, STEAM_NORM_DICT, LOCAL_STEAM_NAMES, STEAM_RAW_DICT)

            if appid:
                s['appid'] = appid
                new_ch_rank.append(s)
                new_ch_sum[str(appid)] = new_ch_sum.get(str(appid), 0) + viewers

        PLATFORM_RANKINGS["chzzk"] = new_ch_rank
        LIVE_STREAMS["chzzk"] = new_ch_sum
        PLATFORM_RANKINGS["last_updated"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f"✅ [Chzzk] 수집 및 정제 완료")
    except Exception as e:
        print(f"❌ [Chzzk] 처리 에러: {e}")


async def update_twitch_rank():
    """트위치 실시간 인기 게임 수집 (매일 02, 08, 14, 20시)"""
    print(f"[{datetime.now()}] 🟣 [Twitch] 정기 수집 시작...")
    await load_steam_dict()
    try:
        client_id, client_secret = "cfvwl8jixo7cf2kvvebd90cg7iwkfk", "rs4lwqj9nrp0j6fi0cxymrigw5llwa"
        token = await asyncio.to_thread(get_twitch_access_token, client_id, client_secret)

        if token:
            tw_list = await asyncio.to_thread(fetch_twitch_ranking, client_id, token, limit=100)
            new_tw_sum, new_tw_rank = {}, []

            for s in tw_list:
                g_name = s.get('game_name')
                appid, _ = await asyncio.to_thread(get_match_tw, g_name, STEAM_NORM_DICT, LOCAL_STEAM_NAMES,
                                                   STEAM_RAW_DICT)

                if appid:
                    s['appid'] = appid
                    new_tw_rank.append(s)
                    new_tw_sum[str(appid)] = 1

            PLATFORM_RANKINGS["twitch"] = new_tw_rank
            LIVE_STREAMS["twitch"] = new_tw_sum
            print(f"✅ [Twitch] 수집 및 정제 완료")
    except Exception as e:
        print(f"❌ [Twitch] 처리 에러: {e}")