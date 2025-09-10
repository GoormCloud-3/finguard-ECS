# main.py
from fastapi import FastAPI, Request
import logging
import time

# ─────────────────────────────────────────────────────────
# 0) 로깅 기본 설정
# ─────────────────────────────────────────────────────────
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logging.getLogger("uvicorn").handlers = logging.getLogger().handlers
logging.getLogger("uvicorn.access").handlers = logging.getLogger().handlers

# ─────────────────────────────────────────────────────────
# 1) FastAPI 앱
# ─────────────────────────────────────────────────────────
app = FastAPI()


# ─────────────────────────────────────────────────────────
# 2) Health 체크
# ─────────────────────────────────────────────────────────
@app.get("/health")
def health():
    return {"status": "ok"}


# ─────────────────────────────────────────────────────────
# 3) Prometheus /metrics 노출 (ADOT가 긁어감)
# ─────────────────────────────────────────────────────────
from prometheus_client import Counter, Histogram, generate_latest, CONTENT_TYPE_LATEST
from starlette.responses import Response

REQUEST_COUNT = Counter(
    "http_requests_total", "Total HTTP requests", ["method", "path", "status"]
)
REQUEST_LATENCY = Histogram(
    "http_request_duration_seconds", "Request latency (seconds)", ["path"]
)


@app.middleware("http")
async def prometheus_middleware(request: Request, call_next):
    # 헬스는 지표에서 제외해도 됨(원하면 조건문으로 제외)
    start = time.time()
    response = await call_next(request)
    latency = time.time() - start

    path = request.url.path
    REQUEST_COUNT.labels(request.method, path, str(response.status_code)).inc()
    REQUEST_LATENCY.labels(path).observe(latency)
    return response


@app.get("/metrics")
def metrics():
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)


# ─────────────────────────────────────────────────────────
# 4) 라우터(include_router)
# ─────────────────────────────────────────────────────────
from api.transaction import router as transaction_router

app.include_router(transaction_router)

# ─────────────────────────────────────────────────────────
# 5) 실행/트레이싱 관련 안내(코드 변경 불필요)
# ─────────────────────────────────────────────────────────
# 트레이스는 OpenTelemetry 자동계측으로 보냄:
#   실행 커맨드 예)  opentelemetry-instrument --traces_exporter otlp --metrics_exporter none \
#                     --service_name transaction-service \
#                     uvicorn main:app --host 0.0.0.0 --port 8000
# 환경변수(태스크 정의에서 주입):
#   OTEL_EXPORTER_OTLP_ENDPOINT=http://127.0.0.1:4317
#   OTEL_EXPORTER_OTLP_PROTOCOL=grpc
#   OTEL_RESOURCE_ATTRIBUTES=service.name=transaction-service,service.namespace=finguard
