FROM python:3.14-slim

WORKDIR /cg20-api

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

ENTRYPOINT ["python", "main.py"]
CMD []