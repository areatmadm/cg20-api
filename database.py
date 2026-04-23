import os
from urllib.parse import quote_plus
from dotenv import load_dotenv

from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker, declarative_base
from motor.motor_asyncio import AsyncIOMotorClient

load_dotenv()

# ==========================================
# 1. MariaDB 설정 (특수기호 완벽 대응)
# ==========================================
DB_USER = os.getenv("MARIADB_USER", "root")
DB_PASS = os.getenv("MARIADB_PASSWORD", "myP@ssw0rd!#?") # 특수기호 들어간 비번 치기...
DB_HOST = os.getenv("MARIADB_HOST", "127.0.0.1")
DB_PORT = os.getenv("MARIADB_PORT", "3306")
DB_NAME = os.getenv("MARIADB_DATABASE", "streamrank")

# 💡 [핵심] 비밀번호만 안전한 문자열로 변환 (예: @ -> %40)
encoded_db_pass = quote_plus(DB_PASS)

# URL 안전하게 조립
MARIADB_URL = f"mysql+aiomysql://{DB_USER}:{encoded_db_pass}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

engine = create_async_engine(MARIADB_URL, echo=False)
AsyncSessionLocal = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
Base = declarative_base()

async def get_rdb():
    async with AsyncSessionLocal() as session:
        yield session

# ==========================================
# 2. MongoDB 설정 (몽고DB도 똑같이 적용 가능!)
# ==========================================
MONGO_USER = os.getenv("MONGO_USER", "mongo_admin")
MONGO_PASS = os.getenv("MONGO_PASSWORD", "m0ng0@pw!")
MONGO_HOST = os.getenv("MONGO_HOST", "127.0.0.1")
MONGO_PORT = os.getenv("MONGO_PORT", "27017")
MONGO_DB_NAME = os.getenv("MONGO_DB_NAME", "streamrank_mongo")

encoded_mongo_pass = quote_plus(MONGO_PASS)

# 몽고DB URL 조립
MONGO_URL = f"mongodb://{MONGO_USER}:{encoded_mongo_pass}@{MONGO_HOST}:{MONGO_PORT}/admin"

mongo_client = AsyncIOMotorClient(MONGO_URL)
mongo_db = mongo_client[MONGO_DB_NAME]

def get_mongodb():
    return mongo_db