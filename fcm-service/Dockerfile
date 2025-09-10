FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    AWS_REGION=ap-northeast-2 \
    AWS_DEFAULT_REGION=ap-northeast-2 \
    OTEL_TRACES_EXPORTER=otlp \
    OTEL_METRICS_EXPORTER=none \
    OTEL_PROPAGATORS=xray \
    TZ=Asia/Seoul

WORKDIR /app

RUN apt-get update && \
    apt-get install -y --no-install-recommends ca-certificates tzdata && \
    rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN python -m pip install --upgrade pip setuptools wheel && \
    pip install --no-cache-dir -r requirements.txt

COPY . .

# secrets/SSM에서 FIREBASE_SA_JSON을 태스크 정의로 주입하고 있으니
# 이미지에는 키 파일 넣지 말기(지금처럼 OK)

CMD ["opentelemetry-instrument", \
     "--service_name", "fcm-service", \
     "python", "worker.py"]
