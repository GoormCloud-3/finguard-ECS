FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    AWS_DEFAULT_REGION=ap-northeast-2 \
    TZ=Asia/Seoul

WORKDIR /app

# pandas/의존성 대비: CA + (휠 없을 때 대비한) 빌드툴
RUN apt-get update && \
    DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
      ca-certificates tzdata curl build-essential && \
    rm -rf /var/lib/apt/lists/*

COPY requirements.txt ./
RUN python -m pip install --upgrade pip setuptools wheel && \
    pip install --no-cache-dir -r requirements.txt

COPY . .

CMD ["opentelemetry-instrument", "--traces_exporter", "otlp", "--metrics_exporter", "none", "--service_name", "sqs-service", "python", "app.py"]
