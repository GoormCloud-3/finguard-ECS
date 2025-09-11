FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    AWS_DEFAULT_REGION=ap-northeast-2 \
    WHATAP_HOME=/whatap

WORKDIR /app

# 기본 SSL CA/네트워킹 유틸
RUN apt-get update && \
    apt-get install -y --no-install-recommends ca-certificates curl && \
    rm -rf /var/lib/apt/lists/*

# Python 의존성 설치
COPY requirements.txt .
RUN python -m pip install --upgrade pip setuptools wheel && \
    pip install --no-cache-dir -r requirements.txt

# 앱 복사
COPY ./app /app
ENV PYTHONPATH=/app

# Whatap 설치 및 설정
RUN pip install --no-cache-dir whatap-python && \
    mkdir -p /whatap 

# 실행 시 환경변수로 whatap.conf 생성
COPY ./docker-entrypoint.sh /usr/local/bin/
RUN chmod +x /usr/local/bin/docker-entrypoint.sh
ENTRYPOINT ["/usr/local/bin/docker-entrypoint.sh"]

# OTel 실행(서비스명은 각 서비스 이미지에서 바꾸세요: account-service/user-service/transaction-service)
CMD ["opentelemetry-instrument", "--traces_exporter", "otlp", "--metrics_exporter", "none", "--service_name", "account-service", "uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
# CMD ["whatap-start-agent","opentelemetry-instrument","--traces_exporter","otlp","--metrics_exporter","none","--service_name","account-service","uvicorn","main:app","--host","0.0.0.0","--port","8000"]
