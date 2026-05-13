FROM python:3.14-slim

WORKDIR /cg20-api

RUN pip install uvicorn app asyncio fastapi apscheduler sqlalchemy playwright lxml httpx dotenv motor aiomysql requests pandas ddgs thefuzz deep_translator elasticsearch
RUN playwright install chromium --with-deps

COPY . .

ENTRYPOINT ["python", "main.py"]
CMD []