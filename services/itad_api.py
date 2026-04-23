import os
import httpx
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession
from datetime import datetime

ITAD_API_KEY = os.getenv("ITAD_API_KEY")


async def get_itad_game_id(appid: int) -> str | None:
    url = f"https://api.isthereanydeal.com/v2/game/lookup/?key={ITAD_API_KEY}&appid={appid}"
    try:
        async with httpx.AsyncClient() as client:
            res = await client.get(url, timeout=5.0)
            data = res.json()
            return data.get('game', {}).get('id') if data.get('found') else None
    except:
        return None


async def sync_itad_price_history(db: AsyncSession, appid: int):
    """3개국 통화에 대해 ITAD 가격 히스토리를 수집하여 적재합니다."""
    itad_id = await get_itad_game_id(appid)
    if not itad_id: return

    for currency in ['KRW', 'JPY', 'USD']:
        country = "KR" if currency == "KRW" else "JP" if currency == "JPY" else "US"
        url = f"https://api.isthereanydeal.com/v2/game/history/?key={ITAD_API_KEY}&id={itad_id}&shops=61&country={country}"

        try:
            async with httpx.AsyncClient() as client:
                res = await client.get(url, timeout=10.0)
                prices = res.json().get('history', [])

                for p in prices:
                    dt = datetime.fromtimestamp(p['t']).strftime('%Y-%m-%d')
                    await db.execute(text("""
                                          INSERT INTO game_price_history (game_id, currency, date, price, regular_price, discount_percent)
                                          VALUES (:gid, :curr, :dt, :p, :rp, :dp) ON DUPLICATE KEY
                                          UPDATE price=
                                          VALUES (price), regular_price=
                                          VALUES (regular_price), discount_percent=
                                          VALUES (discount_percent)
                                          """), {'gid': appid, 'curr': currency, 'dt': dt, 'p': p['p'], 'rp': p['r'],
                                                 'dp': p['d']})
            await db.commit()
            print(f"  📈 [ITAD] AppID {appid} ({currency}) 히스토리 동기화 완료")
        except Exception as e:
            print(f"  ❌ [ITAD] {currency} 에러: {e}")