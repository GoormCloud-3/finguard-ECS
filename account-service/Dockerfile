FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \ 
    PYTHONUNBUFFERED=1 \
    AWS_DEFAULT_REGION=ap-northeast-2

WORKDIR /app

RUN apt-get update && \
    apt-get install -y --no-install-recommends wget curl ca-certificates && \
    rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN python -m pip install --upgrade pip setuptools wheel && \
    pip install --no-cache-dir -r requirements.txt


COPY ./app /app
ENV PYTHONPATH=/app

CMD ["opentelemetry-instrument", "--traces_exporter", "otlp", "--metrics_exporter", "none", "--service_name", "account-service", "uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
