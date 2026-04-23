import asyncio
import requests
import re
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text
from services.itad_api import sync_itad_price_history
from datetime import datetime


# ==========================================
# 📡 1. 스팀 API - 가격 수집 (KRW, JPY, USD)
# ==========================================
async def fetch_price_for_currency(appid: int, currency: str) -> float:
    if currency == 'USD':
        currency = 'US'
    elif currency == 'JPY':
        currency = 'JP'
    else:
        currency = 'KR'
    url = f"https://store.steampowered.com/api/appdetails?appids={appid}&cc={currency}"
    try:
        res = await asyncio.to_thread(requests.get, url, timeout=5)
        data = res.json().get(str(appid), {})
        if not data.get('success'): return 0.0

        gd = data['data']
        if gd.get('is_free'): return 0.0

        # 대표님 DB의 game_prices.price (decimal) 컬럼에 맞게 최종가(final)만 추출
        price_overview = gd.get('price_overview', {})
        return price_overview.get('final', 0) / 100
    except:
        return 0.0


# ==========================================
# 📡 2. 스팀 API - 데이터 상세 수집 (대표님 스키마 맞춤)
# ==========================================
async def fetch_full_steam_data(appid: int) -> dict | str | None:
    """
    결과값 상세:
    - dict: 수집 성공 (게임 상세 정보 포함)
    - "BANNED": 스팀이 'success: false'를 반환 (한국 지역락, 비공개, 또는 삭제된 게임)
    - "RETRY": API 호출 제한(429) 또는 기타 통신 에러 (나중에 다시 시도해야 함)
    """
    url = f"https://store.steampowered.com/api/appdetails?appids={appid}&l=korean&cc=KR"

    try:
        # 1. API 호출
        res = await asyncio.to_thread(requests.get, url, timeout=5)

        # 2. API 제한(Rate Limit) 확인
        if res.status_code == 429:
            print(f"⚠️ [Steam API] {appid} 호출 제한(429). 대기열(RETRY)로 보냅니다.")
            return "RETRY"

        # 3. JSON 데이터 파싱 및 무결성 확인
        json_resp = res.json()
        if not json_resp or str(appid) not in json_resp:
            return "RETRY"  # 응답이 아예 없거나 구조가 깨진 경우 재시도

        data = json_resp[str(appid)]

        # 4. [핵심] 지역락/삭제 여부 확인
        # success가 False인 경우, 해당 국가(KR)에서 조회가 불가능한 게임입니다.
        if not data.get('success'):
            print(f"🛑 [Steam API] {appid}는 한국 지역락 또는 삭제된 게임입니다. (BANNED)")
            return "BANNED"

        # -----------------------------------------
        # 여기서부터는 데이터가 확실히 있는 경우입니다.
        # -----------------------------------------
        gd = data['data']

        # 날짜 포맷 변환 (Steam: '2024년 4월 22일' -> DB: '2024-04-22')
        raw_date = gd.get('release_date', {}).get('date', '')
        clean_date = None
        if raw_date:
            try:
                # '2024년 4월 22일' 형태 파싱 시도 (간단 구현)
                nums = re.findall(r'\d+', raw_date)
                if len(nums) >= 3:
                    clean_date = f"{nums[0]}-{nums[1].zfill(2)}-{nums[2].zfill(2)}"
            except:
                pass

        result = {
            'games': {
                'game_id': appid,
                'game_name': gd.get('name', '')[:500],
                'game_releaseDate': clean_date,
                'game_is_free': 1 if gd.get('is_free') else 0,
                'game_description': gd.get('short_description', '')[:500],
                'header_image_url': gd.get('header_image', '')[:200],
                'os_windows': 1 if gd.get('platforms', {}).get('windows') else 0,
                'os_mac': 1 if gd.get('platforms', {}).get('mac') else 0,
                'os_linux': 1 if gd.get('platforms', {}).get('linux') else 0,
                'app_type': gd.get('type', 'game')[:20],
                'is_gamepad': 1 if gd.get('controller_support') == 'full' else 0,
                'base_game_id': gd.get('fullgame', {}).get('appid') if gd.get('type') == 'dlc' else None
            },
            'genres': [{"id": int(g['id']), "name": g['description']} for g in gd.get('genres', [])],
            'developers': gd.get('developers', []),
            'publishers': gd.get('publishers', []),
            'languages': [],  # 아래에서 파싱
            'prices': {}
        }

        # 언어 파싱 (Full Audio 여부 체크)
        lang_html = gd.get('supported_languages', '')
        if lang_html:
            for l_item in lang_html.split(','):
                l_name = re.sub(r'<[^>]+>', '', l_item).strip().replace('*', '')
                is_voice = 1 if 'audio' in l_item.lower() or 'voice' in l_item.lower() else 0
                if l_name: result['languages'].append({'name': l_name, 'is_voice': is_voice})

        # 3개국 가격 수집 및 결과 반환
        tasks = [fetch_price_for_currency(appid, cc) for cc in ['KRW', 'JPY', 'USD']]
        prices = await asyncio.gather(*tasks)
        result['prices'] = {'KRW': prices[0], 'JPY': prices[1], 'USD': prices[2]}

        return result

    except Exception as e:
        print(f"❌ [Steam API] {appid} 예외 발생: {e}")
        return "RETRY"  # 네트워크 단절 등 알 수 없는 에러는 일단 재시도


# ==========================================
# 💾 3. RDB 적재 (대표님 스키마 100% 반영)
# ==========================================
async def insert_full_game_data(db: AsyncSession, gi: dict):
    appid = gi['games']['game_id']
    is_new_game = False

    try:
        # [상황 2] 없는 게임인지 먼저 확인
        check_res = await db.execute(text("SELECT game_id FROM games WHERE game_id = :gid"), {"gid": appid})
        if not check_res.scalar():
            is_new_game = True

        # 1. games 테이블
        await db.execute(text("""
                              INSERT INTO games (game_id, game_name, game_releaseDate, game_is_free, game_description,
                                                 header_image_url, os_windows, os_mac, os_linux, app_type, is_gamepad,
                                                 base_game_id)
                              VALUES (:game_id, :game_name, :game_releaseDate, :game_is_free, :game_description,
                                      :header_image_url, :os_windows, :os_mac, :os_linux, :app_type, :is_gamepad,
                                      :base_game_id) ON DUPLICATE KEY
                              UPDATE game_name=
                              VALUES (game_name), header_image_url=
                              VALUES (header_image_url)
                              """), gi['games'])

        # 2. game_prices (game_id, currency, price)
        for cc, val in gi['prices'].items():
            await db.execute(text("""
                                  INSERT INTO game_prices (game_id, currency, price)
                                  VALUES (:gid, :cc, :p) ON DUPLICATE KEY
                                  UPDATE price=
                                  VALUES (price)
                                  """), {'gid': appid, 'cc': cc, 'p': val})

        # 3. genres & game_genres
        for g in gi['genres']:
            await db.execute(text("INSERT IGNORE INTO genres (genre_id, genre_name) VALUES (:id, :n)"),
                             {"id": g['id'], "n": g['name']})
            await db.execute(text("INSERT IGNORE INTO game_genres (game_id, genre_id) VALUES (:gid, :zid)"),
                             {"gid": appid, "zid": g['id']})

        # 4. publishers & game_publishers (publisher_name 컬럼명 준수)
        for p in gi['publishers']:
            await db.execute(text("INSERT IGNORE INTO publishers (publisher_name) VALUES (:n)"), {"n": p})
            res = await db.execute(text("SELECT publisher_id FROM publishers WHERE publisher_name=:n"), {"n": p})
            pid = res.scalar()
            if pid: await db.execute(
                text("INSERT IGNORE INTO game_publishers (game_id, publisher_id) VALUES (:gid, :pid)"),
                {"gid": appid, "pid": pid})

        # 5. languages & game_languages (language_name, is_voice 컬럼명 준수)
        for l in gi['languages']:
            await db.execute(text("INSERT IGNORE INTO languages (language_name) VALUES (:n)"), {"n": l['name']})
            res = await db.execute(text("SELECT language_id FROM languages WHERE language_name=:n"), {"n": l['name']})
            lid = res.scalar()
            if lid: await db.execute(
                text("INSERT IGNORE INTO game_languages (game_id, language_id, is_voice) VALUES (:gid, :lid, :iv)"),
                {"gid": appid, "lid": lid, "iv": l['is_voice']})

        # 6. developers & game_developers (추정: developer_name)
        for d in gi['developers']:
            await db.execute(text("INSERT IGNORE INTO developers (developer_name) VALUES (:n)"), {"n": d})
            res = await db.execute(text("SELECT developer_id FROM developers WHERE developer_name=:n"), {"n": d})
            did = res.scalar()
            if did: await db.execute(
                text("INSERT IGNORE INTO game_developers (game_id, developer_id) VALUES (:gid, :did)"),
                {"gid": appid, "did": did})

            # 2. game_prices 적재 및 [상황 1] 가격 변동 체크
            price_changed = False
            for cc, new_price in gi['prices'].items():
                # DB에 저장된 기존 가격 가져오기
                old_res = await db.execute(text("SELECT price FROM game_prices WHERE game_id=:gid AND currency=:cc"),
                                           {"gid": appid, "cc": cc})
                old_price = old_res.scalar()

                # 💡 가격이 다르다면 트리거 ON!
                if old_price is not None and float(old_price) != float(new_price):
                    price_changed = True

                await db.execute(text("""
                                      INSERT INTO game_prices (game_id, currency, price)
                                      VALUES (:gid, :cc, :p) ON DUPLICATE KEY
                                      UPDATE price=
                                      VALUES (price)
                                      """), {'gid': appid, 'cc': cc, 'p': new_price})

            await db.commit()
        print(f"🌟 [DB 적재완료] AppID {appid} ('{gi['games']['game_name']}')")
    except Exception as e:
        await db.rollback()
        print(f"❌ [DB 적재실패] AppID {appid}: {e}")