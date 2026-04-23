import requests
import time
import pandas as pd
import re
from ddgs import DDGS
from thefuzz import process, fuzz
from deep_translator import GoogleTranslator

# 번역기 초기화 (트위치는 영문 위주이므로 en->ko로 설정하신 것 유지하되 필요시 자동 감지)
translator = GoogleTranslator(source='auto', target='en')


# ----------------------------------------------------
# 💡 특수문자/공백/기호를 싹 다 지우는 제초 함수
# ----------------------------------------------------
def clean(text):
    if not text: return ""
    return re.sub(r'[^a-z0-9가-힣]', '', str(text).lower())


# ==========================================
# 📡 트위치 API 전용 함수
# ==========================================
def get_twitch_access_token(client_id, client_secret):
    url = "https://id.twitch.tv/oauth2/token"
    params = {
        "client_id": client_id,
        "client_secret": client_secret,
        "grant_type": "client_credentials"
    }
    try:
        res = requests.post(url, params=params).json()
        return res.get("access_token")
    except:
        return None


def fetch_twitch_ranking(client_id, access_token, limit=100):
    print(f"💜 [Twitch] 실시간 인기 게임 Top {limit} 수집 중...")
    url = "https://api.twitch.tv/helix/games/top"
    headers = {
        "Client-ID": client_id,
        "Authorization": f"Bearer {access_token}"
    }
    params = {"first": 50}
    result_data = []

    for _ in range(limit // 50):
        res = requests.get(url, headers=headers, params=params).json()
        data = res.get('data', [])
        for item in data:
            result_data.append({
                "rank": len(result_data) + 1,
                "game_name": item['name'],
                "id": item['id']
            })
        cursor = res.get('pagination', {}).get('cursor')
        if cursor:
            params['after'] = cursor
        else:
            break
    return result_data[:limit]


# ==========================================
# 🔍 스팀 검증 & 검색 엔진 함수
# ==========================================
def find_appid_via_search(game_name):
    query = f"site:store.steampowered.com/app/ {game_name}"
    try:
        with DDGS() as ddgs:
            results = list(ddgs.text(query, max_results=3))
            for res in results:
                url = res.get('href', '')
                match = re.search(r'store\.steampowered\.com/app/(\d+)', url)
                if match: return int(match.group(1))
    except:
        pass
    return None


def verify_game_with_steam(appid, original_name):
    url = f"https://store.steampowered.com/api/appdetails?appids={appid}&l=korean"
    try:
        res = requests.get(url, timeout=5).json()
        data = res.get(str(appid), {})
        if not data.get('success'): return False, "페이지 없음"
        steam_official_name = data['data']['name']

        c_orig = clean(original_name)
        c_steam = clean(steam_official_name)

        if c_orig in c_steam or c_steam in c_orig or fuzz.ratio(c_orig, c_steam) >= 80:
            return True, f"검증 통과 ('{steam_official_name}')"
        return False, f"이름 불일치 (스팀: {steam_official_name})"
    except:
        return False, "API 오류"


# ==========================================
# 🎯 4단계 워터폴 매칭 엔진
# ==========================================
def get_final_match(game_name, steam_norm_dict, local_steam_names, steam_raw_dict):
    c_name = clean(game_name)

    # 0. 제외 키워드 필터링
    black_list = ["리그오브레전드", "고전게임", "종합게임", "마인크래프트", "justchatting"]
    if c_name in black_list:
        return None, "❌ 제외 카테고리"

    # 1. 로컬 DB 완벽일치
    if c_name in steam_norm_dict:
        return steam_norm_dict[c_name], "⚡ Phase 1: 로컬 완벽일치"

    # 2. 번역 후 로컬 일치 (트위치는 영문 위주라 번역이 덜 쓰이겠지만 보험용)
    translated = game_name
    if re.search(r'[가-힣]', game_name):
        try:
            translated = translator.translate(game_name)
            c_trans = clean(translated)
            if c_trans in steam_norm_dict:
                return steam_norm_dict[c_trans], f"🌐 Phase 2: 번역 일치 ('{translated}')"
        except:
            pass

    # 3. 로컬 DB 포함/유사도 검증
    c_target = clean(translated)
    matches = process.extract(translated, local_steam_names, scorer=fuzz.token_set_ratio, limit=15)
    for match in matches:
        m_name, score = match[0], match[1]
        c_found = clean(m_name)
        if c_target in c_found and len(c_found) >= len(c_target):
            return steam_raw_dict[m_name], f"🎯 Phase 3: 포함 확인 ('{m_name}')"

    # 4. 덕덕고 검색
    search_appid = find_appid_via_search(game_name)
    if search_appid:
        is_valid, reason = verify_game_with_steam(search_appid, game_name)
        if is_valid:
            return search_appid, f"✅ Phase 4: 검색 성공 ({reason})"

    return None, "❌ 모든 단계 실패"


# ==========================================
# 🚀 메인 실행부
# ==========================================
if __name__ == "__main__":
    STEAM_DB_FILE = "master_rdb.csv"
    TWITCH_CLIENT_ID = "cfvwl8jixo7cf2kvvebd90cg7iwkfk"
    TWITCH_CLIENT_SECRET = "rs4lwqj9nrp0j6fi0cxymrigw5llwa"

    print("=" * 70)
    print("🎬 트위치 - 스팀 랭킹 매칭 엔진 가동")
    print("=" * 70)

    # [중요] 1. 스팀 DB 로드 및 인덱싱 (이게 빠져서 에러가 났던 겁니다!)
    try:
        steam_df = pd.read_csv(STEAM_DB_FILE)
        print(f"📦 원본 스팀 DB({STEAM_DB_FILE}) 로드 완료.")

        # 컬럼명 유연하게 찾기
        n_col = 'game_name' if 'game_name' in steam_df.columns else 'name'

        steam_norm_dict = {}
        steam_raw_dict = {}
        for _, row in steam_df.iterrows():
            r_name = str(row.get(n_col, '')).strip()
            appid = row.get('appid', row.get('game_id', row.get('app_id')))
            if pd.notna(appid) and r_name:
                cleaned_key = clean(r_name)
                steam_norm_dict[cleaned_key] = int(appid)
                steam_raw_dict[r_name] = int(appid)
        local_steam_names = list(steam_raw_dict.keys())

    except FileNotFoundError:
        print(f"❌ 에러: {STEAM_DB_FILE} 파일이 없습니다!")
        exit()

    # 2. 트위치 토큰 및 랭킹 수집
    token = get_twitch_access_token(TWITCH_CLIENT_ID, TWITCH_CLIENT_SECRET)
    if not token:
        print("❌ 트위치 토큰 발급 실패!")
        exit()

    twitch_games = fetch_twitch_ranking(TWITCH_CLIENT_ID, token, limit=100)

    # 3. 매칭 시작
    print(f"\n🔍 트위치 게임 {len(twitch_games)}개 매칭을 시작합니다...\n")

    for game in twitch_games:
        appid, log = get_final_match(game['game_name'], steam_norm_dict, local_steam_names, steam_raw_dict)
        print(f"[{game['rank']}위] {game['game_name']} -> ID: {appid} ({log})")
        if "Phase 4" in log: time.sleep(1.2)