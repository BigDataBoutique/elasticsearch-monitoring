FROM python:3.5.5-slim

WORKDIR /app
COPY requirements.txt ./

ENV PYTHONPATH=/app

RUN pip install --no-cache-dir -r requirements.txt

COPY . .