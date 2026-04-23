from fastapi import APIRouter
from services.crowel_twitch import fetch_twitch_ranking

router = APIRouter(
    prefix="/api/twitch",
    tags=["Twitch Ranking"]
)

@router.get("/current-rank")
def get_twitch_rank():
    data = fetch_twitch_ranking("CLIENT_ID", "TOKEN", limit=10)
    return {"status": "success", "platform": "twitch", "data": data}