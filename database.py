import os
from urllib.parse import quote_plus
from dotenv import load_dotenv
from sqlalchemy import text

from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker, declarative_base
from motor.motor_asyncio import AsyncIOMotorClient
from elasticsearch import AsyncElasticsearch

load_dotenv()

# ==========================================
# 1. MariaDB 설정 (특수기호 완벽 대응)
# ==========================================
DB_USER = os.getenv("MARIADB_USER", "root")
DB_PASS = os.getenv("MARIADB_PASSWORD", "pass123#") # 특수기호 들어간 비번 치기...
DB_HOST = os.getenv("MARIADB_HOST", "192.168.0.114")
DB_PORT = os.getenv("MARIADB_PORT", "3306")
DB_NAME = os.getenv("MARIADB_DATABASE", "gamedb")

# 💡 [핵심] 비밀번호만 안전한 문자열로 변환 (예: @ -> %40)
encoded_db_pass = quote_plus(DB_PASS)

# URL 안전하게 조립
MARIADB_URL = f"mysql+aiomysql://{DB_USER}:{encoded_db_pass}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

engine = create_async_engine(MARIADB_URL, echo=False)
AsyncSessionLocal = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
Base = declarative_base()
'''
async def get_rdb():
    try:
    async with AsyncSessionLocal() as session:
        yield session
        await session.execute(text("SELECT 1"))
    print ("✅ MariaDB 연결 성공!")
    except Exception as e:
        print(f"❌ MariaDB 연결 실패: {e}")
'''

async def get_rdb():
    async with AsyncSessionLocal() as session:
        yield session

async def connect_to_rdb():
    """FastAPI 구동 시 MariaDB 연결 확인"""
    try:
        async with AsyncSessionLocal() as session:
            await session.execute(text("SELECT 1"))
        print("✅ MariaDB 연결 성공!")
    except Exception as e:
        print(f"❌ MariaDB 연결 실패: {e}")        

    
ES_HOST = os.getenv("ES_HOST", "localhost")
ES_PORT = os.getenv("ES_PORT", "9200")

es = AsyncElasticsearch(f"http://{ES_HOST}:{ES_PORT}")

async def connect_to_es():
    try:
        exists = await es.indices.exists(index="games")
        if not exists:
            await es.indices.create(index="games", body={
                "mappings": {
                    "properties": {
                        "game_id":    {"type": "integer"},
                        "game_name":  {"type": "text", "analyzer": "standard"},
                        "genres":     {"type": "keyword"},
                        "developers": {"type": "text", "analyzer": "standard"},
                        "publishers": {"type": "text", "analyzer": "standard"},
                        "is_free":    {"type": "boolean"},
                        "price_krw":  {"type": "float"},
                        "header_image_url": {"type": "keyword"}
                    }
                }
            })
            print("✅ ES 인덱스 생성 완료")
        print("✅ Elasticsearch 연결 성공!")
    except Exception as e:
        print(f"❌ Elasticsearch 연결 실패: {e}")

async def close_es():
    await es.close()
# ==========================================
# 2. MongoDB 설정 (직접 주입 방식으로 변경!)
# ==========================================
MONGO_USER = os.getenv("MONGO_USER", "mongo_admin")
MONGO_PASS = os.getenv("MONGO_PASS", "m0ng0@pw!")
MONGO_HOST = os.getenv("MONGO_HOST", "127.0.0.1")
MONGO_PORT = os.getenv("MONGO_PORT", "27017")
MONGO_DB_NAME = os.getenv("MONGO_DB_NAME", "streamrank_mongo")

# URL 조립 (가장 스탠다드한 방식 복구)
from urllib.parse import quote_plus

encoded_pass = quote_plus(MONGO_PASS)
# 💡 authSource=admin 명시
MONGO_URL = f"mongodb://{MONGO_USER}:{encoded_pass}@{MONGO_HOST}:{MONGO_PORT}/?authSource=admin"


class MongoDB:
    client: AsyncIOMotorClient = None
    db = None


db_obj = MongoDB()


async def connect_to_mongo():
    """FastAPI 구동 시 호출할 연결 함수"""
    print("⏳ MongoDB 비동기 연결 시도 중...")
    db_obj.client = AsyncIOMotorClient(MONGO_URL)
    db_obj.db = db_obj.client[MONGO_DB_NAME]

    # 💡 핑을 때려서 실제로 연결됐는지 확실히 검증합니다.
    try:
        await db_obj.client.admin.command('ping')
        print("✅ MongoDB 연결 성공!")
    except Exception as e:
        print(f"❌ MongoDB 연결 실패: {e}")


async def close_mongo_connection():
    if db_obj.client:
        db_obj.client.close()
        print("🛑 MongoDB 연결 종료")


# 다른 파일에서 가져다 쓸 수 있도록 래퍼 함수 생성
def get_mongodb():
    return db_obj.db