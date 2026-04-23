import asyncio

import httpx
import requests
import re
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text
from database import get_mongodb
from services.itad_api import sync_itad_price_history
from datetime import datetime


# ==========================================
# 📡 1. 스팀 뉴스 & 리뷰 추가 수집기
# ==========================================
async def fetch_steam_news_and_reviews(appid: int) -> dict:
    """스팀 뉴스(3건)와 최신 리뷰를 가져옵니다."""
    news_url = f"https://api.steampowered.com/ISteamNews/GetNewsForApp/v2/?appid={appid}&count=3"
    reviews_url = f"https://store.steampowered.com/appreviews/{appid}?json=1&language=ko"

    results = {"news": [], "reviews": []}

    try:
        async with httpx.AsyncClient() as client:
            # 1. 뉴스 가져오기
            news_res = await client.get(news_url, timeout=5.0)
            if news_res.status_code == 200:
                items = news_res.json().get('appnews', {}).get('newsitems', [])
                for item in items:
                    results['news'].append({
                        "title": item.get('title'),
                        "url": item.get('url'),
                        "date": item.get('date')  # Unix Timestamp
                    })

            # 2. 리뷰 가져오기
            rev_res = await client.get(reviews_url, timeout=5.0)
            if rev_res.status_code == 200:
                rev_list = rev_res.json().get('reviews', [])
                for r in rev_list:
                    results['reviews'].append({
                        "is_positive": r.get('voted_up'),
                        "playtime_hours": r.get('author', {}).get('playtime_forever', 0) / 60,
                        "content": r.get('review'),
                        "date": r.get('timestamp_created')  # Unix Timestamp
                    })
    except Exception as e:
        print(f"  ⚠️ [Steam Sub-Data] {appid} 수집 중 일부 누락: {e}")

    return results

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

            # =========================================================
            # 🎯 각종 외부 API 트리거 발동 (ITAD, 뉴스, 리뷰)
            # =========================================================
            if is_new_game:
                # 1. ITAD 히스토리 수집
                print(f"  🆕 [신규 게임 발견] AppID {appid}의 ITAD 히스토리를 수집합니다.")
                await sync_itad_price_history(db, appid)

                # 💡 2. [추가] 신규 게임은 뉴스/리뷰도 강제로 한 번 긁어서 몽고DB에 넣어줍니다!
                print(f"  🆕 [신규 게임 발견] AppID {appid}의 뉴스/리뷰 초기 데이터를 몽고DB에 적재합니다.")

                # 같은 파일에 있는 개별 수집기 함수들을 여기서 호출합니다.
                initial_news = await fetch_steam_news_only(appid, count=3)
                await save_game_news_to_mongo(appid, initial_news)

                initial_reviews = await fetch_steam_reviews_only(appid, count=10)
                await save_game_reviews_to_mongo(appid, initial_reviews)

            elif price_changed:
                # 가격 변동 시에는 ITAD만 수집 (뉴스/리뷰는 안 건드림)
                print(f"  💰 [가격 변동 감지] AppID {appid}의 ITAD 히스토리를 갱신합니다.")
                await sync_itad_price_history(db, appid)

        print(f"🌟 [DB 적재완료] AppID {appid} ('{gi['games']['game_name']}')")
    except Exception as e:
        await db.rollback()
        print(f"❌ [DB 적재실패] AppID {appid}: {e}")


# ==========================================
# 💾 4. MongoDB 개별 적재기 (뉴스 & 리뷰)
# ==========================================
async def save_game_news_to_mongo(game_id: int, news_items: list):
    """수집된 뉴스를 MongoDB에 Upsert 합니다."""
    if not news_items: return

    mongo_db = get_mongodb()

    try:
        await mongo_db.game_news.update_one(
            {"game_id": game_id},
            {"$set": {
                "game_id": game_id,
                "news": news_items,
                "updated_at": datetime.now()
            }},
            upsert=True
        )
        print(f"  🌟 [MongoDB] AppID {game_id} 뉴스 적재 완료")
    except Exception as e:
        print(f"  ❌ [MongoDB] AppID {game_id} 뉴스 적재 실패: {e}")


async def save_game_reviews_to_mongo(game_id: int, reviews: list):
    """수집된 리뷰를 MongoDB에 Upsert 합니다."""
    if not reviews: return

    mongo_db = get_mongodb()

    try:
        await mongo_db.game_reviews.update_one(
            {"game_id": game_id},
            {"$set": {
                "game_id": game_id,
                "reviews": reviews,
                "updated_at": datetime.now()
            }},
            upsert=True
        )
        print(f"  🌟 [MongoDB] AppID {game_id} 리뷰 적재 완료")
    except Exception as e:
        print(f"  ❌ [MongoDB] AppID {game_id} 리뷰 적재 실패: {e}")

# ==========================================
# 📡 4. 뉴스 & 리뷰 개별 수집기 (온디맨드용)
# ==========================================
async def fetch_steam_news_only(appid: int, count: int = 3) -> list:
    url = f"https://api.steampowered.com/ISteamNews/GetNewsForApp/v2/?appid={appid}&count={count}"
    try:
        async with httpx.AsyncClient() as client:
            res = await client.get(url, timeout=5.0)
            if res.status_code == 200:
                return res.json().get('appnews', {}).get('newsitems', [])
    except: pass
    return []

async def fetch_steam_reviews_only(appid: int, count: int = 10) -> list:
    # 💡 쿼리 파라미터에 num_per_page=10 등을 넣어 개수를 조절합니다.
    url = f"https://store.steampowered.com/appreviews/{appid}?json=1&language=all&num_per_page={count}"
    try:
        async with httpx.AsyncClient() as client:
            res = await client.get(url, timeout=5.0)
            if res.status_code == 200:
                rev_list = res.json().get('reviews', [])
                # 필요한 필드만 정제해서 반환
                return [{
                    "is_positive": r.get('voted_up'),
                    "playtime_hours": r.get('author', {}).get('playtime_forever', 0) / 60,
                    "content": r.get('review'),
                    "date": r.get('timestamp_created')
                } for r in rev_list]
    except: pass
    return []