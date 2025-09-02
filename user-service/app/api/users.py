# api/user.py
import logging
from typing import Sequence

import anyio
from fastapi import APIRouter
from fastapi.responses import JSONResponse
from pymysql.err import IntegrityError, OperationalError

from db.rds import get_connection
from models.schema import UserResponse, UserRequest

# ✅ OpenTelemetry 로 전환
from opentelemetry import trace
from opentelemetry.trace import Status, StatusCode

router = APIRouter()
tracer = trace.get_tracer("account-service")  # 서비스명은 OTEL_RESOURCE_ATTRIBUTES로도 설정됨

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
    - FastAPI OTel 계측으로 요청 인바운드 컨텍스트는 자동 추출됨
    - DB 구간을 명시적 span 으로 감싸서 가시성 강화
    - 블로킹 DB는 anyio.to_thread.run_sync 로 오프로딩
    """
    conn = None
    try:
        conn = get_connection()
        user_sub = payload.userSub
        gps_location: Sequence[float] | None = payload.gps_location

        # 입력 검증
        if not user_sub or not gps_location:
            return JSONResponse(
                status_code=400,
                content={"error": "BadRequest", "message": "userSub 또는 gps_location 누락"},
            )
        if not isinstance(gps_location, (list, tuple)) or len(gps_location) != 2:
            return JSONResponse(
                status_code=400,
                content={"error": "BadRequest", "message": "gps_location 형식은 [lat, lon] 이어야 합니다."},
            )

        lat, lon = gps_location[0], gps_location[1]
        # MySQL WKT는 "POINT(lon lat)" 순서
        point_wkt = f"POINT({lon} {lat})"

        # DB INSERT 구간 트레이싱
        with tracer.start_as_current_span("sql.insert_user") as span:
            span.set_attribute("db.system", "mysql")
            span.set_attribute("app.user.sub", user_sub)
            span.set_attribute("db.statement", "INSERT INTO users(userSub, gps_location) VALUES (?, ST_PointFromText(?))")
            try:
                await anyio.to_thread.run_sync(_insert_user, conn, user_sub, point_wkt)
            except Exception as e:
                span.record_exception(e)
                span.set_status(Status(StatusCode.ERROR, str(e)))
                raise

        return {
            "message": "Sign up successful. Please verify your email or phone if required.",
            "userSub": user_sub,
        }

    except IntegrityError as e:
        return JSONResponse(
            status_code=409,
            content={"error": "UsernameExistsException", "message": "해당 아이디는 이미 존재합니다."},
        )
    except OperationalError as e:
        logging.exception("DB connection/operation error")
        return JSONResponse(
            status_code=503,
            content={"error": "ServiceUnavailable", "message": "DB 연결 문제로 요청을 처리할 수 없습니다."},
        )
    except Exception:
        logging.exception("User registration error")
        return JSONResponse(
            status_code=400,
            content={"error": "BadRequest", "message": "회원가입 처리 중 에러가 발생했습니다."},
        )
    finally:
        try:
            if conn:
                conn.close()
        except Exception:
            logging.error("Failed to close DB connection")
