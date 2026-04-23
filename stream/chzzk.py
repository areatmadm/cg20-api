from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession
from database import get_rdb # 아까 만든 DB 세션 함수
from services.crowel_chzzk import fetch_chzzk_ranking_official, get_final_match

# 접두사(prefix)를 달아서 /api/chzzk 로 들어오는 요청만 여기서 처리
router = APIRouter(
    prefix="/api/chzzk",
    tags=["Chzzk Ranking"]
)

@router.get("/current-rank")
def get_chzzk_rank():
    # 1. 서비스 모듈에서 크롤링 함수 호출
    top_games = fetch_chzzk_ranking_official(limit=10)

    # 2. 결과 가공해서 JSON으로 바로 내뱉기 (FastAPI가 알아서 변환해줌)
    return {"status": "success", "platform": "chzzk", "data": top_games}

@router.post("/run-matching")
async def run_chzzk_matching(db: AsyncSession = Depends(get_rdb)):
    top_games = fetch_chzzk_ranking_official(limit=100)

    for game in top_games:
        # 1. 매칭 로직 수행 (appid 찾기)
        appid, log = get_final_match(...)

        if appid:
            # 2. 찾은 appid를 DB에 우아하게 저장 (FK 에러 핸들링)
            await save_stream_rank(
                db=db,
                platform="chzzk",
                appid=appid,
                num_stream=game.get('live_count', 0),
                totalviewers=game.get('viewers', 0)
            )

    return {"status": "success", "msg": "치지직 랭킹 DB 저장 완료"}