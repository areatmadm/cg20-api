import httpx
from lxml import html
import re

async def fetch_steam_chart_httpx(country_code: str) -> list:
    url = f"https://store.steampowered.com/charts/topselling/{country_code}?l=koreana"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept-Language": "ko-KR,ko;q=0.9",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    }

    try:
        async with httpx.AsyncClient(follow_redirects=True) as client:
            res = await client.get(url, headers=headers, timeout=15.0)
            res.raise_for_status()
            html_content = res.text

        tree = html.fromstring(html_content)
        links = tree.xpath('//table//a[contains(@href, "/app/")]/@href')

        appids = []
        for link in links:
            match = re.search(r'/app/(\d+)', link)
            if match:
                appids.append(int(match.group(1)))

        unique_appids = list(dict.fromkeys(appids))
        print(f"  [{country_code}] 추출된 AppID: {len(unique_appids)}개")

        if unique_appids:
            print(f"  [{country_code}] 샘플 5개: {unique_appids[:5]}")
        else:
            print(f"  [{country_code}] ❌ AppID 없음 → JS 렌더링 필요할 수 있음")
            # 디버깅용: HTML 일부 출력
            print(f"  [{country_code}] HTML 길이: {len(html_content)}")
            print(f"  [{country_code}] HTML 앞부분: {html_content[:500]}")

        return unique_appids[:100]

    except Exception as e:
        print(f"  [{country_code}] 에러: {e}")
        return []


import asyncio

async def main():
    print("=== 스팀 차트 httpx 테스트 ===\n")
    for country in ["KR", "JP", "US"]:
        result = await fetch_steam_chart_httpx(country)
        print(f"  [{country}] 최종 결과: {len(result)}개\n")

asyncio.run(main())