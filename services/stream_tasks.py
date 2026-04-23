# services/stream_tasks.py
import asyncio
from sqlalchemy import text
from datetime import datetime
from store import LIVE_STREAMS, PLATFORM_RANKINGS
from database import AsyncSessionLocal

# 💡 대표님의 크롤러 (이제 에러 없이 루트에서 가져옵니다)
from .crowel_chzzk import fetch_chzzk_ranking_official, get_final_match as get_match_ch, clean, get_final_match
from .crowel_twitch import fetch_twitch_ranking, get_twitch_access_token, get_final_match as get_match_tw

# 매칭을 위한 3대장 사전
STEAM_NORM_DICT = {}  # 제초된 이름 -> AppID
STEAM_RAW_DICT = {}  # 원본 이름 -> AppID
LOCAL_STEAM_NAMES = []  # 유사도 검사용 리스트


async def load_steam_dict():
    """MariaDB에서 스팀 게임 목록을 가져와 매칭 사전을 만듭니다."""
    global STEAM_NORM_DICT, STEAM_RAW_DICT, LOCAL_STEAM_NAMES
    if STEAM_NORM_DICT: return

    try:
        async with AsyncSessionLocal() as db:
            result = await db.execute(text("SELECT game_id, game_name FROM games"))
            for row in result.fetchall():
                appid, name = int(row[0]), str(row[1]).strip()
                STEAM_NORM_DICT[clean(name)] = appid
                STEAM_RAW_DICT[name] = appid
            LOCAL_STEAM_NAMES = list(STEAM_RAW_DICT.keys())
        print(f"✅ 스팀 매칭 사전 로드 완료 ({len(STEAM_NORM_DICT)}개)")
    except Exception as e:
        print(f"❌ 사전 로드 실패: {e}")


async def update_chzzk_rank():
    """치지직 수집 -> 4단계 워터폴 매칭 -> 스팀 게임만 저장"""
    await load_steam_dict()
    print(f"[{datetime.now()}] 🟢 [Chzzk] 수집 및 매칭 시작...")

    try:
        # 원본 함수 호출 (100개 수집)
        ch_list = await asyncio.to_thread(fetch_chzzk_ranking_official, limit=100)
        new_ch_sum, new_ch_rank = {}, []

        for s in ch_list:
            # 💡 [중요] 매 루프마다 AppID 변수 초기화 (전염 방지)
            current_appid = None
            g_name = s.get('chzzk_game_name')
            viewers = int(s.get('viewers', 0))

            # 4단계 워터폴 매칭 엔진 가동 (원본 로직)
            current_appid, log = await asyncio.to_thread(
                get_final_match, g_name, STEAM_NORM_DICT, LOCAL_STEAM_NAMES, STEAM_RAW_DICT
            )

            # 🛑 진짜 스팀 AppID가 확보된 경우에만 랭킹에 포함!
            if current_appid and isinstance(current_appid, int):
                s['appid'] = current_appid
                new_ch_rank.append(s)
                new_ch_sum[str(current_appid)] = new_ch_sum.get(str(current_appid), 0) + viewers
            else:
                # 스팀 게임이 아닌 것들은 여기서 걸러짐 (로그 확인용)
                # print(f"  ⏩ [Skip] {g_name}: {log}")
                pass

        PLATFORM_RANKINGS["chzzk"] = new_ch_rank
        LIVE_STREAMS["chzzk"] = new_ch_sum
        # 💡 [핵심 추가] 두 캐시 모두 시간 도장을 쾅 찍어줍니다!
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        PLATFORM_RANKINGS["last_updated"] = current_time
        LIVE_STREAMS["last_updated"] = current_time

        print(f"✅ [Chzzk] 정제 완료: {len(new_ch_rank)}개 게임 등재")

    except Exception as e:
        print(f"❌ [Chzzk] 치명적 에러: {e}")

async def update_twitch_rank():
    """트위치 인기 게임 수집 (스팀 매칭 필터링 강화)"""
    print(f"[{datetime.now()}] 🟣 [Twitch] 수집 시작...")
    await load_steam_dict()
    try:
        client_id, client_secret = "cfvwl8jixo7cf2kvvebd90cg7iwkfk", "rs4lwqj9nrp0j6fi0cxymrigw5llwa"
        token = await asyncio.to_thread(get_twitch_access_token, client_id, client_secret)

        if token:
            tw_list = await asyncio.to_thread(fetch_twitch_ranking, client_id, token, limit=100)
            new_tw_sum, new_tw_rank = {}, []

            for s in tw_list:
                current_appid = None # 💡 초기화!
                g_name = s.get('game_name')

                current_appid, log = await asyncio.to_thread(
                    get_match_tw, g_name, STEAM_NORM_DICT, LOCAL_STEAM_NAMES, STEAM_RAW_DICT
                )

                if current_appid:
                    s['appid'] = current_appid
                    new_tw_rank.append(s)
                    new_tw_sum[str(current_appid)] = 1

            PLATFORM_RANKINGS["twitch"] = new_tw_rank
            LIVE_STREAMS["twitch"] = new_tw_sum
            # 💡 [핵심 추가] 여기도 두 캐시 모두 시간 도장을 쾅 찍어줍니다!
            current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            PLATFORM_RANKINGS["last_updated"] = current_time
            LIVE_STREAMS["last_updated"] = current_time
    except Exception as e:
        print(f"❌ [Twitch] 에러: {e}")