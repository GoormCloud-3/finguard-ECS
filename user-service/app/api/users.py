# api/user.py (예시 경로 — 파일명은 네 프로젝트 구조에 맞춰 사용)
import logging
from typing import Sequence

import anyio
from fastapi import APIRouter, HTTPException
from fastapi.responses import JSONResponse
from pymysql.err import IntegrityError, OperationalError

from db.rds import get_connection
from models.schema import UserResponse, UserRequest
from aws_xray_sdk.core import xray_recorder

router = APIRouter()

# ----- 스레드풀에서 실행될 순수 DB 함수 (X-Ray 호출 금지) -----

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
    - X-Ray 세그먼트: 미들웨어에서 요청마다 자동 생성
    - 여기서는 필요한 구간만 in_subsegment 로 감싼다.
    - DB 작업은 블로킹이므로 anyio.to_thread.run_sync 로 스레드풀에 오프로딩
    """
    conn = None
    try:
        conn = get_connection()
        logging.info("Starting create user")

        user_sub = payload.userSub
        gps_location = payload.gps_location  # 기대: [lat, lon] 또는 (lat, lon)

        # 입력 검증
        if not user_sub or not gps_location:
            logging.error("Missing userSub or gps_location in request")
            return JSONResponse(
                status_code=400,
                content={"error": "BadRequest", "message": "userSub 또는 gps_location 누락"},
            )

        if not isinstance(gps_location, (list, tuple)) or len(gps_location) != 2:
            logging.error("Invalid gps_location format: %s", gps_location)
            return JSONResponse(
                status_code=400,
                content={"error": "BadRequest", "message": "gps_location 형식은 [lat, lon] 이어야 합니다."},
            )

        lat, lon = gps_location[0], gps_location[1]
        # MySQL WKT는 "POINT(lon lat)" 순서
        point_wkt = f"POINT({lon} {lat})"

        logging.info(f"🔍 Creating user with userSub: {user_sub}")

        # DB INSERT 구간 트레이싱
        with xray_recorder.in_subsegment("sql:insert_user"):
            logging.info(f"💾 Inserting user into DB: {user_sub}, {point_wkt}")
            await anyio.to_thread.run_sync(_insert_user, conn, user_sub, point_wkt)

        logging.info(f"✅ User created successfully: {user_sub}, {point_wkt}")

        return {
            "message": "Sign up successful. Please verify your email or phone if required.",
            "userSub": user_sub,
        }

    except IntegrityError as e:
        logging.warning("User already exists: %s", e)
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
                logging.info("🔚 Closing DB connection")
                conn.close()
        except Exception:
            logging.error("Failed to close DB connection")
            pass
