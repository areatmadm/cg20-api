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


async def update_live_streams():
    """스케줄러가 호출할 메인 함수 (게임 랭킹 기반)"""
    print(f"[{datetime.now()}] 🔴 스트리밍 게임 랭킹 수집 시작...")
    await load_steam_dict()

    # 1. 치지직(Chzzk) 게임 랭킹 처리
    try:
        ch_list = await asyncio.to_thread(fetch_chzzk_ranking_official, limit=100)
        new_ch_sum = {}
        new_ch_rank = []

        for s in ch_list:
            # 💡 수정: 크롤러가 주는 키값('game_name')과 일치 확인
            g_name = s.get('game_name') or s.get('chzzk_game_name')
            viewers = int(s.get('viewers', 0))

            # 🛑 1단계: 매칭 엔진을 통과한 녀석만 '스팀 게임'으로 인정합니다.
            appid, log = await asyncio.to_thread(
                get_match_ch, g_name, STEAM_NORM_DICT, LOCAL_STEAM_NAMES, STEAM_RAW_DICT
            )

            # 🛑 2단계: appid가 있는 경우에만 '진짜 랭킹' 리스트에 넣습니다.
            if appid:
                s['appid'] = appid  # 데이터에 ID 박제
                new_ch_rank.append(s)  # 여기서 넣어야 '리그 오브 레전드'가 걸러집니다!

                # 시청자 수 합산
                new_ch_sum[str(appid)] = new_ch_sum.get(str(appid), 0) + viewers
            else:
                # 스팀에 없는 게임은 로그만 남기고 버립니다.
                # print(f"🚫 [Skip] 스팀 미등록 게임 제외: {g_name} ({log})")
                continue

        PLATFORM_RANKINGS["chzzk"] = new_ch_rank  # 이제 여기엔 '진짜 스팀 게임'만 남습니다.
        LIVE_STREAMS["chzzk"] = new_ch_sum
    except Exception as e:
        print(f"❌ 치지직 처리 에러: {e}")

    # 2. 트위치(Twitch) 게임 랭킹 처리
    try:
        client_id = "cfvwl8jixo7cf2kvvebd90cg7iwkfk"
        client_secret = "rs4lwqj9nrp0j6fi0cxymrigw5llwa"

        token = await asyncio.to_thread(get_twitch_access_token, client_id, client_secret)
        if token:
            # 1. 트위치 인기 게임 100개를 가져옵니다.
            tw_list = await asyncio.to_thread(fetch_twitch_ranking, client_id, token, limit=100)
            new_tw_rank = []
            new_tw_sum = {}

            for s in tw_list:
                g_name = s.get('game_name')
                if not g_name: continue

                # 🛑 [핵심 수정] 매칭 엔진을 먼저 돌려 AppID를 확보합니다.
                appid, log = await asyncio.to_thread(
                    get_match_tw, g_name, STEAM_NORM_DICT, LOCAL_STEAM_NAMES, STEAM_RAW_DICT
                )

                # 🛑 [핵심 수정] 스팀 AppID가 확인된 게임만 리스트에 추가합니다!
                if appid:
                    s['appid'] = appid  # 프론트엔드 연동을 위해 ID 박제
                    new_tw_rank.append(s)

                    # 트위치 게임 랭킹 API는 시청자 수를 바로 주지 않으므로
                    # 랭킹에 존재함을 알리기 위해 1 또는 순위 가중치를 부여합니다.
                    new_tw_sum[str(appid)] = 1
                else:
                    # 스팀에 없는 'Just Chatting'이나 'LoL' 등은 여기서 조용히 걸러집니다.
                    # print(f"🚫 [Twitch Skip] {g_name} -> 스팀 미등록 카테고리")
                    continue

            # 정제된 데이터만 전역 스토어에 반영
            PLATFORM_RANKINGS["twitch"] = new_tw_rank
            LIVE_STREAMS["twitch"] = new_tw_sum

    except Exception as e:
        print(f"❌ 트위치 처리 에러: {e}")

    current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    PLATFORM_RANKINGS["last_updated"] = current_time
    LIVE_STREAMS["last_updated"] = current_time
    print(f"✅ 스트리밍 게임 랭킹 갱신 완료")