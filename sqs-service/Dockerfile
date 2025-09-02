FROM python:3.11-slim

# ---- 기본 환경 ----
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    AWS_DEFAULT_REGION=ap-northeast-2 \
    TZ=Asia/Seoul

WORKDIR /app


RUN apt-get update && \
    apt-get install -y curl ca-certificates tzdata openssl && \
    rm -rf /var/lib/apt/lists/*

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

    
COPY . .

CMD ["opentelemetry-instrument", "--traces_exporter", "otlp", "--metrics_exporter", "none", "--service_name", "sqs-service", "python", "app.py"]
