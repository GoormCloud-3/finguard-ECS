FROM python:3.11-slim

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    AWS_REGION=ap-northeast-2 \
    AWS_DEFAULT_REGION=ap-northeast-2 \
    XRAY_SERVICE_NAME=fcm-service

RUN apt-get update && \
    apt-get install -y --no-install-recommends ca-certificates && \
    rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN python -m pip install --upgrade pip setuptools wheel && \
    pip install --no-cache-dir -r requirements.txt

COPY . .

CMD ["opentelemetry-instrument", "--traces_exporter", "otlp", "--metrics_exporter", "none", "--service_name", "fcm-service", "python", "worker.py"]
