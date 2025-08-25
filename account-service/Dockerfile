FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \ 
    PYTHONUNBUFFERED=1 \
    AWS_DEFAULT_REGION=ap-northeast-2
# PYTHONDONTWRITEBYTECODE=1
# .pyc 바이트코드 파일을 생성하지 않도록 함. 컨테이너 환경에서 불필요한 캐시 파일 생성을 방지.

# PYTHONUNBUFFERED=1
# stdout/stderr 출력을 버퍼링하지 않고 바로 출력. 로그가 실시간으로 확인 가능 (ECS, CloudWatch 등에서 중요).

# AWS_DEFAULT_REGION=ap-northeast-2
# boto3 같은 AWS SDK에서 기본 리전을 지정. region을 명시하지 않아도 SDK가 이 값을 사용.

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
# 설치 확인용 스크립트.
# aws-xray-sdk가 설치됐는지 확인.
# aws_xray_sdk.ext 하위 모듈 리스트 확인 → Starlette용 미들웨어가 포함됐는지 확인 가능.
# 설치 후 필요한 X-Ray SDK와 Starlette 확장이 있는지 검증하는 단계

# 4) 앱 코드 복사
COPY ./app /app

ENV PYTHONPATH=/app

ENTRYPOINT ["python", "-m", "uvicorn"]
CMD ["main:app", "--host", "0.0.0.0", "--port", "8000", "--loop", "asyncio", "--http", "h11", "--workers", "1"]

