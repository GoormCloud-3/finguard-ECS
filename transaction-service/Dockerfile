# ---- Base: 안정적인 3.11-slim 권장
FROM python:3.11-slim

# ---- 기본 ENV (역슬래시 뒤 공백 절대 금지!)
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    AWS_DEFAULT_REGION=ap-northeast-2 \
    OTEL_TRACES_EXPORTER=otlp \
    OTEL_METRICS_EXPORTER=none \
    OTEL_PROPAGATORS=xray

WORKDIR /app

# ---- 필수 패키지(인증서/헬스체크용 curl)
RUN apt-get update && \
    apt-get install -y --no-install-recommends curl ca-certificates && \
    rm -rf /var/lib/apt/lists/*

# ---- 파이썬 의존성
COPY requirements.txt .
RUN python -m pip install --upgrade pip setuptools wheel && \
    pip install --no-cache-dir -r requirements.txt

# ---- 앱 소스
COPY ./app /app
ENV PYTHONPATH=/app

# ---- 실행 (서비스명만 바꿔서 빌드별로 사용)
#    예) user-service / account-service / transaction-service
CMD ["opentelemetry-instrument", \
     "--service_name", "transaction-service", \
     "uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
