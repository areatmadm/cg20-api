from fastapi import FastAPI
from routers import chzzk, twitch

app = FastAPI(title="StreamRank API Engine")

# 만들어둔 모듈들을 조립!
app.include_router(chzzk.router)
app.include_router(twitch.router)

@app.get("/")
def read_root():
    return {"msg": "StreamRank API 서버 가동 중🚀"}

# uvicorn main:app --reload 로 실행