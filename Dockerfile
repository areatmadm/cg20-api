FROM python:3.14-slim

WORKDIR /cg20-api

#RUN pip install --upgrade pip==26.1.1
RUN pip install uvicorn app asyncio fastapi apscheduler sqlalchemy playwright lxml httpx dotenv motor aiomysql requests pandas ddgs thefuzz deep_translator
RUN playwright install chromium --with-deps

COPY . .

CMD ["uvicorn", "main:app", "--host", "0.0.0.0"]