import os
from dotenv import load_dotenv
from urllib.parse import quote_plus
from motor.motor_asyncio import AsyncIOMotorClient
import asyncio

# 1. .env 파일 로드
load_dotenv()


async def debug_env_and_auth():
    print("=" * 50)
    print("🔍 .env 파일에서 읽어온 원본 값 (대괄호 안을 보세요)")
    print("=" * 50)

    # 💡 getenv로 직접 확인
    user = os.getenv("MONGO_USER")
    pw = os.getenv("MONGO_PASSWORD")
    host = os.getenv("MONGO_HOST")
    port = os.getenv("MONGO_PORT")
    db_name = os.getenv("MONGO_DB_NAME")

    # 대괄호 [] 를 붙여서 앞뒤 공백이나 따옴표가 있는지 확인합니다.
    print(f"USER:     [{user}]")
    print(f"PASSWORD: [{pw}]")
    print(f"HOST:     [{host}]")
    print(f"PORT:     [{port}]")
    print(f"DB_NAME:  [{db_name}]")
    print("=" * 50)

    if not pw:
        print("❌ 에러: 비밀번호를 읽어오지 못했습니다. .env 파일 위치를 확인하세요.")
        return

    # 2. 읽어온 값으로 실제 연결 시도
    print("\n🚀 위 값들을 가지고 실제 비동기 연결을 시도합니다...")

    # 특수문자 처리를 위해 인코딩 적용
    enc_pw = quote_plus(pw)
    # authSource=admin 명시
    uri = f"mongodb://{user}:{enc_pw}@{host}:{port}/?authSource=admin"

    try:
        client = AsyncIOMotorClient(uri, serverSelectionTimeoutMS=2000)
        # 실제로 핑을 날려봐야 인증이 되었는지 알 수 있습니다.
        await client.admin.command('ping')
        print("✅ [최종 성공] .env 설정값으로 인증에 성공했습니다!")
        print("💡 만약 여기서 성공하는데 서버(FastAPI)에서 안 된다면, main.py의 실행 시점이 문제입니다.")
    except Exception as e:
        print(f"❌ [최종 실패] 이 값으로는 인증이 안 됩니다: {e}")
        print("\n🆘 [조치사항]")
        if '"' in str(pw) or "'" in str(pw):
            print("👉 비번에 따옴표가 섞여있습니다! .env에서 따옴표를 지우세요.")
        if '#' in str(pw):
            print("👉 비번에 #이 있습니다! .env에서 비번 전체를 큰따옴표로 감싸보세요.")


if __name__ == "__main__":
    asyncio.run(debug_env_and_auth())