import requests
import time
import pandas as pd
import re
from ddgs import DDGS
from thefuzz import process, fuzz
from deep_translator import GoogleTranslator

# 번역기 초기화
translator = GoogleTranslator(source='ko', target='en')

# ----------------------------------------------------
# 💡 [핵심] 특수문자/공백/기호를 싹 다 지우는 제초 함수
# ----------------------------------------------------
def clean(text):
    if not text: return ""
    # 알파벳, 숫자, 한글만 남기고 나머지(®, ™, :, -, 공백 등) 싹 제거
    return re.sub(r'[^a-z0-9가-힣]', '', str(text).lower())


def verify_game_with_steam(appid, chzzk_name):
    url = f"https://store.steampowered.com/api/appdetails?appids={appid}&l=korean"
    try:
        res = requests.get(url, timeout=5).json()
        data = res.get(str(appid), {})
        if not data.get('success'): return False, "페이지 없음"

        steam_official_name = data['data']['name']

        # 💡 검증 단계에서도 '제초' 후 비교
        c_chzzk = clean(chzzk_name)
        c_steam = clean(steam_official_name)

        # 포함되거나(오버워치 2), 거의 비슷하면 합격
        if c_chzzk in c_steam or c_steam in c_chzzk or fuzz.ratio(c_chzzk, c_steam) >= 80:
            return True, f"검증 통과 ('{steam_official_name}')"

        # 번역 검증 (최후의 수단)
        trans_chzzk = clean(translator.translate(chzzk_name))
        if trans_chzzk in c_steam or fuzz.ratio(trans_chzzk, c_steam) >= 80:
            return True, f"번역 검증 통과 ('{steam_official_name}')"

        return False, f"이름 불일치 (스팀: {steam_official_name})"
    except:
        return False, "API 오류"


# ==========================================
# 1. 치지직 랭킹 수집 (무한 스크롤)
# ==========================================
def fetch_chzzk_ranking_official(limit=100):
    print(f"🟩 [Chzzk] 실시간 인기 게임 Top {limit} 수집 중...")
    headers = {"User-Agent": "Mozilla/5.0"}
    url = "https://api.chzzk.naver.com/service/v1/categories/live"
    params = {"categoryType": "GAME", "size": 50}
    result_data = []
    rank_counter = 1

    while len(result_data) < limit:
        try:
            res = requests.get(url, headers=headers, params=params).json()
            if res.get('code') != 200: break
            data_list = res.get('content', {}).get('data', [])
            if not data_list: break

            for item in data_list:
                if len(result_data) >= limit: break
                result_data.append({
                    "rank": rank_counter,
                    "chzzk_game_name": str(item.get('categoryValue', '')).strip(),
                    "viewers": item.get('concurrentUserCount', 0)
                })
                rank_counter += 1

            next_cursor = res.get('content', {}).get('page', {}).get('next')
            if next_cursor:
                params.update(next_cursor)
            else:
                break
        except Exception:
            break
    return result_data


# ==========================================
# 2. 덕덕고 검색 엔진 하이재킹
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
    except Exception:
        pass
    return None


# ==========================================
# 3. 스팀 API & 번역기 융합 검증기
# ==========================================
def verify_game_with_steam(appid, chzzk_name):
    url = f"https://store.steampowered.com/api/appdetails?appids={appid}&l=korean"
    try:
        res = requests.get(url, timeout=5).json()
        data = res.get(str(appid), {})
        if not data.get('success'):
            return False, "스팀 상점 페이지 비공개/삭제됨"

        steam_official_name = data['data']['name']

        # 1차: 원본 그대로 유사도 검사
        score = fuzz.token_sort_ratio(chzzk_name.lower(), steam_official_name.lower())
        if score >= 60:
            return True, f"유사도 {score}% ('{steam_official_name}')"

        # 2차: 한글 포함 시 영어로 번역해서 재검사
        if re.search(r'[가-힣]', chzzk_name):
            try:
                translated_name = translator.translate(chzzk_name)
                trans_score = fuzz.token_sort_ratio(translated_name.lower(), steam_official_name.lower())
                if trans_score >= 70:
                    return True, f"번역 통과 {trans_score}% ('{translated_name}' ≈ '{steam_official_name}')"
            except Exception:
                pass

        return False, f"유사도 미달 {score}% (스팀: '{steam_official_name}')"
    except Exception as e:
        return False, f"API 통신 오류"


# ==========================================
# 🎯 [핵심] 4단계 워터폴 매칭 로직 분리
# ==========================================
def get_final_match(chzzk_name, steam_norm_dict, local_steam_names, steam_raw_dict):
    c_chzzk = clean(chzzk_name)

    # 1. 로컬 DB 직행 (제초 매칭)
    if c_chzzk in steam_norm_dict:
        return steam_norm_dict[c_chzzk], "⚡ Phase 1: 로컬 완벽일치"

    # 2. 번역 후 로컬 DB 일치 확인
    translated_name = chzzk_name
    if re.search(r'[가-힣]', chzzk_name):
        try:
            translated_name = translator.translate(chzzk_name)
            c_trans = clean(translated_name)
            if c_trans in steam_norm_dict:
                return steam_norm_dict[c_trans], f"🌐 Phase 2: 번역 일치 ('{translated_name}')"
        except:
            pass

    # 3. 로컬 DB 유사도 + 포함 관계 (제초 버전)
    c_target = clean(translated_name)
    # 후보 15개 추출
    matches = process.extract(translated_name, local_steam_names, scorer=fuzz.token_set_ratio, limit=15)

    for match in matches:
        m_name, score = match[0], match[1]
        c_found = clean(m_name)

        # 💡 [핵심] 특수문자 없는 상태에서 '포함' 유무 확인 (오버워치 구출)
        if c_target in c_found:
            # 낚시 방지: 찾은 이름이 타겟보다 너무 짧으면 기각 (메이플-스토리 방지)
            if len(c_found) >= len(c_target):
                return steam_raw_dict[m_name], f"🎯 Phase 3: 포함 확인 ('{m_name}')"

    # 4. 덕덕고 검색 (로컬에 진짜 없을 때만)
    search_appid = find_appid_via_search(chzzk_name)
    if search_appid:
        is_valid, reason = verify_game_with_steam(search_appid, chzzk_name)
        if is_valid:
            return search_appid, f"✅ Phase 4: 검색 성공 ({reason})"

    return None, "❌ 모든 단계 실패"


# ==========================================
# 🚀 메인 실행부 (컬럼명 game_name 대응)
# ==========================================
if __name__ == "__main__":
    STEAM_DB_FILE = "master_rdb.csv"
    steam_df = pd.read_csv(STEAM_DB_FILE)

    # 컬럼명 유연하게 찾기
    n_col = 'game_name' if 'game_name' in steam_df.columns else 'name'

    steam_norm_dict = {}
    steam_raw_dict = {}
    for _, row in steam_df.iterrows():
        r_name = str(row.get(n_col, '')).strip()
        appid = row.get('appid', row.get('game_id', row.get('app_id')))
        if pd.notna(appid) and r_name:
            # 인덱싱할 때도 '제초' 해서 넣기
            steam_norm_dict[clean(r_name)] = int(appid)
            steam_raw_dict[r_name] = int(appid)

    local_steam_names = list(steam_raw_dict.keys())
    top_games = fetch_chzzk_ranking_official(limit=100)

    for game in top_games:
        appid, log = get_final_match(game['chzzk_game_name'], steam_norm_dict, local_steam_names, steam_raw_dict)
        print(f"[{game['rank']}위] {game['chzzk_game_name']} -> ID: {appid} ({log})")
        if "Phase 4" in log: time.sleep(1.2)