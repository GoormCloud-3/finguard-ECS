FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    AWS_REGION=ap-northeast-2 \
    AWS_DEFAULT_REGION=ap-northeast-2 \
    XRAY_SERVICE_NAME=fcm-service \
    WHATAP_HOME=/whatap

WORKDIR /app

RUN apt-get update && \
    DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
      ca-certificates tzdata curl build-essential && \
    rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN python -m pip install --upgrade pip setuptools wheel && \
    pip install --no-cache-dir -r requirements.txt

COPY . .

# Whatap 설치 및 설정
RUN pip install --no-cache-dir whatap-python && \
    mkdir -p /whatap 

# 실행 시 환경변수로 whatap.conf 생성
COPY ./docker-entrypoint.sh /usr/local/bin/
RUN chmod +x /usr/local/bin/docker-entrypoint.sh
ENTRYPOINT ["/usr/local/bin/docker-entrypoint.sh"]

CMD ["whatap-start-agent","opentelemetry-instrument","--traces_exporter","otlp","--metrics_exporter","none","--service_name","fcm-service","python","worker.py"]
# CMD ["opentelemetry-instrument","--traces_exporter","otlp","--metrics_exporter","none","--service_name","fcm-service","python","worker.py"]
