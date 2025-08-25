from fastapi import FastAPI, Request
import logging

from aws_xray_sdk.core import xray_recorder, patch
from aws_xray_sdk.core.async_context import AsyncContext

# 1) starlette 미들웨어 있나 시도
try:
    from aws_xray_sdk.ext.starlette.middleware import XRayMiddleware as _XRayMiddleware
except Exception:
    _XRayMiddleware = None

from starlette.middleware.base import BaseHTTPMiddleware  # ✅ 폴백용
EXCLUDE_PATHS = {"/health"}


app = FastAPI()
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logging.getLogger("uvicorn").handlers = logging.getLogger().handlers
logging.getLogger("uvicorn.access").handlers = logging.getLogger().handlers

# X-Ray 설정
xray_recorder.configure(
    service="user-service",   # 서비스 이름(맵에 찍힐 이름)
    context=AsyncContext(),
    sampling=False,    
)
patch(('boto3',))  # boto3/requests 자동 계측

# 2) 폴백 미들웨어 (앱 객체를 교체하지 않음!)
class FallbackXRayMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        if request.url.path in EXCLUDE_PATHS:
            return await call_next(request)
        
        name = f"{request.method} {request.url.path}"
        xray_recorder.begin_segment(name)
        try:
            response = await call_next(request)
            seg = xray_recorder.current_segment()
            if seg:
                seg.put_http_meta("status", response.status_code)
            return response
        except Exception as e:
            seg = xray_recorder.current_segment()
            if seg:
                seg.put_annotation("error", True)
                seg.add_exception(e)
            raise
        finally:
            xray_recorder.end_segment()


# 3) starlette 미들웨어 있으면 그걸 쓰고, 없으면 폴백 추가
if _XRayMiddleware:
    app.add_middleware(_XRayMiddleware, recorder=xray_recorder)
else:
    logging.warning("Using fallback X-Ray middleware (ext.starlette not found).")
    app.add_middleware(FallbackXRayMiddleware)

# (선택) 런타임에서 실제 상태 로깅
try:
    import importlib.metadata as md, aws_xray_sdk, pkgutil
    ver = md.version("aws-xray-sdk")
    has_star = False
    try:
        import aws_xray_sdk.ext as E
        has_star = any(m.name == "starlette" for m in pkgutil.iter_modules(E.__path__))
    except Exception:
        pass
    logging.info(f"[X-Ray] version={ver}, module_path={aws_xray_sdk.__file__}, has_ext_starlette={has_star}")
except Exception as e:
    logging.warning(f"[X-Ray] version check failed: {e}")

# ✅ 이제 자유롭게 라우터 추가 가능 (앱을 바꿔치기 안 했기 때문)
from api.users import router as user_router
app.include_router(user_router)

@app.get("/health")
def health():
    return {"health": "ok"}
