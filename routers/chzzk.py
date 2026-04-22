from fastapi import APIRouter
# 우리가 만든 치지직 서비스 모듈(함수)을 임포트!
from services.chzzk_steam import fetch_chzzk_ranking_official, get_final_match 

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
def run_chzzk_matching():
    # DB 매칭 파이프라인 수동 트리거 API 같은 것도 만들 수 있습니다.
    return {"status": "running", "msg": "치지직-스팀 매칭 백그라운드 작업 시작"}