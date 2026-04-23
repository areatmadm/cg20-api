# scrapers.py
import asyncio
import httpx
from lxml import html
import re
from datetime import datetime, timedelta
from sqlalchemy import text
from playwright.sync_api import sync_playwright

from database import AsyncSessionLocal
from services.steam_api import fetch_full_steam_data, insert_full_game_data
from store import PENDING_QUEUE


# ==========================================
# 🎮 [내부 함수] 동기식(Sync) 가상 브라우저 실행기
# ==========================================
def _fetch_steam_top_sellers_sync(country_code: str) -> list:
    """별도의 쓰레드에서 안전하게 실행될 동기식 Playwright 로직입니다."""
    url = f"https://store.steampowered.com/charts/topselling/{country_code}?l=koreana"

    try:
        # async with 대신 일반 with 사용
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            context = browser.new_context(
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                locale="ko-KR",
                viewport={"width": 1920, "height": 1080}
            )
            page = context.new_page()

            print(f"  🌐 [{country_code}] 가상 브라우저(격리 모드) 시동 중...")

            # 렌더링 완료 대기
            page.goto(url, wait_until='networkidle', timeout=15000)
            html_content = page.content()
            browser.close()

            # HTML 파싱
            tree = html.fromstring(html_content)
            links = tree.xpath('//table//a[contains(@href, "/app/")]/@href')

            appids = []
            for link in links:
                match = re.search(r'/app/(\d+)', link)
                if match:
                    appids.append(int(match.group(1)))

            unique_appids = list(dict.fromkeys(appids))
            print(f"  📡 [{country_code}] 차트 추출 성공: {len(unique_appids)}개")
            return unique_appids[:100]

    except Exception as e:
        print(f"  ❌ [{country_code}] 브라우저 수집 에러: {e}")
        return []


async def fetch_hana_bank_rates() -> dict | None:
    now = datetime.now()
    if now.hour < 7:
        target_dt = now - timedelta(days=1)
        print(f"  🕒 [환율수집] 새벽 시간대. 어제({target_dt.strftime('%Y-%m-%d')}) 환율 조회.")
    else:
        target_dt = now
        print(f"  🕒 [환율수집] 주간 시간대. 오늘({target_dt.strftime('%Y-%m-%d')}) 환율 조회.")

    target_date = target_dt.strftime('%Y%m%d')
    target_date_dash = target_dt.strftime('%Y-%m-%d')

    url = "https://www.hanabank.com/cms/rate/wpfxd651_01i_01.do"

    post_data = {
        'ajax': 'true',
        'curCd': '',
        'tmpInqStrDt': target_date_dash,
        'pbldDvCd': '3',
        'pbldSqn': '',
        'hid_key_data': '',
        'inqStrDt': target_date,
        'inqKindCd': '1',
        'hid_enc_data': '',
        'requestTarget': 'searchContentDiv',
    }

    headers = {
        'Accept': 'text/html, */*; q=0.01',
        'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'X-Requested-With': 'XMLHttpRequest',
    }

    try:
        async with httpx.AsyncClient() as client:
            res = await client.post(url, data=post_data, headers=headers, timeout=10.0)
            res.raise_for_status()
            html_content = res.text
    except Exception as e:
        print(f"  ❌ 하나은행 API 통신 에러: {e}")
        return None

    if not html_content: return None

    tree = html.fromstring(html_content)
    rows = tree.xpath("//table[contains(@class, 'tblBasic')]/tbody/tr")

    if not rows: return None

    rates = {}
    for row in rows:
        currency_nodes = row.xpath(".//td[1]")
        if not currency_nodes: continue

        currency_text = currency_nodes[0].text_content().strip()

        # 💡 오직 매매기준율(9번째 td)만 깔끔하게 가져옵니다!
        if 'USD' in currency_text:
            std_nodes = row.xpath(".//td[9]")
            if std_nodes:
                rates['standard_usd'] = float(std_nodes[0].text_content().replace(',', ''))

        elif 'JPY' in currency_text:
            std_nodes = row.xpath(".//td[9]")
            if std_nodes:
                rates['standard_jpy'] = float(std_nodes[0].text_content().replace(',', ''))

    return rates


# ==========================================
# 🎮 스팀 랭킹 호출 함수 (FastAPI 메인 루프용)
# ==========================================
async def fetch_steam_top_sellers(country_code: str) -> list:
    """
    FastAPI의 비동기 환경을 망가뜨리지 않도록,
    위의 동기식 함수를 '별도의 쓰레드(Thread)'로 던져서 실행합니다.
    """
    # 💡 asyncio.to_thread가 핵심 마법입니다!
    return await asyncio.to_thread(_fetch_steam_top_sellers_sync, country_code)


async def fetch_all_steam_rankings() -> dict:
    """
    한/미/일 스팀 랭킹을 수집하고 DB와 동기화합니다.
    1. DB 존재 여부 확인 -> 2. 미존재 시 API 수집 -> 3. 지역락(Skip) 처리
    """
    print(f"\n[{datetime.now()}] 🎮 --- 스팀 글로벌 랭킹 동기화 시작 ---")

    # 1. 3개국 랭킹 비동기로 동시에 가져오기
    kr_task = fetch_steam_top_sellers('KR')
    jp_task = fetch_steam_top_sellers('JP')
    us_task = fetch_steam_top_sellers('US')
    kr_appids, jp_appids, us_appids = await asyncio.gather(kr_task, jp_task, us_task)

    # 2. 모든 국가의 AppID를 합치고 중복 제거
    all_appids = set(kr_appids + jp_appids + us_appids)

    valid_appids = set()
    banned_appids = set()

    async with AsyncSessionLocal() as db:
        # [Step 1] DB에 이미 있는 게임 한꺼번에 조회
        if all_appids:
            query = text("SELECT game_id FROM games WHERE game_id IN :appids")
            result = await db.execute(query, {"appids": tuple(all_appids)})
            existing_appids = {row[0] for row in result}
            valid_appids.update(existing_appids)
            new_appids = all_appids - existing_appids
            print(f"  🔍 분석 완료: 기존 게임 {len(existing_appids)}개, 신규 게임 {len(new_appids)}개 확인.")
        else:
            new_appids = set()

        # 💡 [핵심 수정] 이 아래의 for 루프가 if/else와 같은 라인(밖)에 있어야 합니다!
        # [Step 2 & 3] 새로운 게임 수집 루프
        for appid in new_appids:
            result = await fetch_full_steam_data(appid)

            if isinstance(result, dict):
                # 1. 수집 성공!
                await insert_full_game_data(db, result)
                PENDING_QUEUE.pop(appid, None)
                valid_appids.add(appid)
                print(f"  ✨ [신규등록] AppID {appid} 저장 완료.")

            elif result == "BANNED":
                # 2. 🛑 영구 제외 (지역락 등)
                banned_appids.add(appid)
                PENDING_QUEUE.pop(appid, None)  # 대기열에 있었다면 삭제
                print(f"  🛑 [영구제외] AppID {appid}는 한국 스토어 미지원 게임입니다.")

            elif result == "RETRY":
                # 3. ⏳ 일시적 실패 (대기열행)
                if appid not in PENDING_QUEUE:
                    PENDING_QUEUE[appid] = {
                        'retry_count': 0,
                        'last_attempt': datetime.now()
                    }
                print(f"  ⏳ [대기열행] AppID {appid} - API 제한으로 나중에 다시 합니다.")

            await asyncio.sleep(1.0)

    # 4. 최종 랭킹 리스트 구성 (한국 스토어에서 유효한 게임들만 필터링)
    final_kr = [app for app in kr_appids if app in valid_appids]
    final_jp = [app for app in jp_appids if app in valid_appids]
    final_us = [app for app in us_appids if app in valid_appids]

    print(f"[{datetime.now()}] 🎮 --- 스팀 글로벌 랭킹 동기화 완료 ---")
    return {
        "KR": final_kr,
        "JP": final_jp,
        "US": final_us,
        "banned_count": len(banned_appids)
    }