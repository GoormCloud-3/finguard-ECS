FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    AWS_DEFAULT_REGION=ap-northeast-2

WORKDIR /app

# 1) requirements 먼저
COPY requirements.txt .

# 2) OS 패키지
RUN apt-get update && \
    apt-get install -y --no-install-recommends curl ca-certificates && \
    rm -rf /var/lib/apt/lists/*

# 3) 패키지 설치 + 검증
RUN python -m pip install --upgrade pip setuptools wheel && \
    pip install --no-cache-dir --upgrade --force-reinstall -r requirements.txt
RUN python - <<'PY'
from importlib.metadata import version, PackageNotFoundError
import pkgutil
try:
    v = version("aws-xray-sdk")
except PackageNotFoundError:
    v = "NOT INSTALLED"
print("xray-sdk:", v)
import aws_xray_sdk.ext as E
print("ext:", [m.name for m in pkgutil.iter_modules(E.__path__)])
PY

# 4) 앱 코드 복사
COPY ./app /app

ENV PYTHONPATH=/app

ENTRYPOINT ["python", "-m", "uvicorn"]
CMD ["main:app", "--host", "0.0.0.0", "--port", "8000", "--loop", "asyncio", "--http", "h11", "--workers", "1"]

