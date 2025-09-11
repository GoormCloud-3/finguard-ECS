# api/user.py
import logging
from typing import Sequence

import anyio
from fastapi import APIRouter
from fastapi.responses import JSONResponse
from pymysql.err import IntegrityError, OperationalError

from db.rds import get_connection
from models.schema import UserResponse, UserRequest

# OpenTelemetry
from opentelemetry import trace
from opentelemetry.trace import Status, StatusCode

# Prometheus
from prometheus_client import Counter

router = APIRouter()
# 서비스명은 태스크 정의의 OTEL_RESOURCE_ATTRIBUTES(service.name=user-service)와 일치시켜야 함
tracer = trace.get_tracer("user-service")

# 간단한 API 요청 카운터 (라벨: path, result)
API_REQUESTS = Counter(
    "user_api_requests_total",
    "user API requests",
    ["path", "result"],  # result: success|bad_request|conflict|unavailable|error
)


# ----- 스레드풀에서 실행될 순수 DB 함수 -----
def _insert_user(conn, user_sub: str, point_wkt: str) -> None:
    with conn.cursor() as cursor:
        cursor.execute(
            "INSERT INTO users (userSub, gps_location) VALUES (%s, ST_PointFromText(%s))",
            (user_sub, point_wkt),
        )
    conn.commit()


# ---------------------------- 라우트 ----------------------------
@router.post("/users", response_model=UserResponse)
async def create_user(payload: UserRequest):
    """
    - FastAPI OTel 자동계측을 켰다면 인바운드 HTTP 스팬은 자동 생성
    - DB 구간은 명시적 span 으로 감싸서 가시성 강화
    - 블로킹 DB는 anyio.to_thread.run_sync 로 오프로딩
    """
    conn = None
    try:
        logging.info("⚙️ Starting createUser API")
        conn = get_connection()
        user_sub = payload.userSub
        gps_location: Sequence[float] | None = payload.gps_location

        # 입력 검증
        if not user_sub or not gps_location:
            API_REQUESTS.labels(path="/users", result="bad_request").inc()
            return JSONResponse(
                status_code=400,
                content={
                    "error": "BadRequest",
                    "message": "userSub 또는 gps_location 누락",
                },
            )
        if not isinstance(gps_location, (list, tuple)) or len(gps_location) != 2:
            API_REQUESTS.labels(path="/users", result="bad_request").inc()
            return JSONResponse(
                status_code=400,
                content={
                    "error": "BadRequest",
                    "message": "gps_location 형식은 [lat, lon] 이어야 합니다.",
                },
            )

        lat, lon = gps_location[0], gps_location[1]
        # MySQL WKT는 "POINT(lon lat)" 순서
        point_wkt = f"POINT({lon} {lat})"

        # DB INSERT 구간 트레이싱
        with tracer.start_as_current_span("sql.insert_user") as span:
            logging.info(
                f"Inserting user {user_sub} with location {point_wkt} into database ..."
            )
            span.set_attribute("db.system", "mysql")
            span.set_attribute("app.user.sub", user_sub)
            # 보안/성능상 전체 SQL은 축약 기록
            span.set_attribute("db.statement", "INSERT INTO users(...) VALUES(...)")
            try:
                await anyio.to_thread.run_sync(_insert_user, conn, user_sub, point_wkt)
            except Exception as e:
                logging.exception("🚨 Error inserting user into database")
                span.record_exception(e)
                span.set_status(Status(StatusCode.ERROR, str(e)))
                API_REQUESTS.labels(path="/users", result="error").inc()
                raise

        API_REQUESTS.labels(path="/users", result="success").inc()
        logging.info(f"✅ User {user_sub} inserted successfully.")
        return {
            "message": "Sign up successful. Please verify your email or phone if required.",
            "userSub": user_sub,
        }

    except IntegrityError:
        logging.warning("🚨 Username already exists")
        API_REQUESTS.labels(path="/users", result="conflict").inc()
        return JSONResponse(
            status_code=409,
            content={
                "error": "UsernameExistsException",
                "message": "해당 아이디는 이미 존재합니다.",
            },
        )
    except OperationalError:
        logging.exception("🚨 DB connection/operation error")
        API_REQUESTS.labels(path="/users", result="unavailable").inc()
        return JSONResponse(
            status_code=503,
            content={
                "error": "ServiceUnavailable",
                "message": "DB 연결 문제로 요청을 처리할 수 없습니다.",
            },
        )
    except Exception:
        logging.exception("🚨 User registration error")
        API_REQUESTS.labels(path="/users", result="error").inc()
        return JSONResponse(
            status_code=400,
            content={
                "error": "BadRequest",
                "message": "회원가입 처리 중 에러가 발생했습니다.",
            },
        )
    finally:
        try:
            if conn:
                conn.close()
        except Exception:
            logging.error("🚨 Failed to close DB connection")
