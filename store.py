# 환율 캐시
LATEST_RATES = {
    "standard_usd": 0.0,
    "standard_jpy": 0.0,
    "last_updated": None
}

# 스팀 랭킹 캐시
LATEST_STEAM_RANKS = {
    "KR": [],
    "JP": [],
    "US": [],
    "last_updated": None
}

# 수집 실패 대기열
PENDING_QUEUE = {}

# 🔴 실시간 방송 캐시
LIVE_STREAMS = {
    "chzzk": {},    # 예: {"1963610": [{"streamer": "풍월량", "viewers": 15000}], ...}
    "twitch": {},   # 예: {"1963610": [{"streamer": "shroud", "viewers": 30000}], ...}
    "last_updated": None
}

# 2. 플랫폼별 전체 랭킹 (오직 시청자 수만 리스트 형태로 보관)
PLATFORM_RANKINGS = {
    "chzzk": [],    # 예: [15000, 12000, 9500, ...] (시청자 수만 저장)
    "twitch": [],
    "last_updated": None
}