# app/api/transaction.py
import uuid, json, math
import logging
from datetime import datetime
from typing import Optional, Tuple
import anyio

from fastapi import APIRouter, HTTPException, Request

from db.rds import get_connection
from api.minMaxHeap import MinHeap, MaxHeap
from models.schema import (
    TransactionRequest, TransactionSuccessResponse, TransactionDetail,
    TransactionMap
)

import boto3
from botocore.config import Config

# OpenTelemetry (X-Ray SDK 대신)
from opentelemetry import trace
from opentelemetry.trace import Status, StatusCode

router = APIRouter()
region = "ap-northeast-2"

_boto_cfg = Config(
    retries={"max_attempts": 10, "mode": "standard"},
    connect_timeout=5,
    read_timeout=30,
)

sqs_client = boto3.client("sqs", region_name=region, config=_boto_cfg)
ssm_client = boto3.client("ssm", region_name=region, config=_boto_cfg)

tracer = trace.get_tracer("transaction-service")


# ---------------- 공용 유틸 (비-DB) ----------------

def haversine_distance(coord1, coord2) -> float:
    R = 6371
    lat1, lon1 = coord1
    lat2, lon2 = coord2
    dLat = math.radians(lat2 - lat1)
    dLon = math.radians(lon2 - lon1)
    a = math.sin(dLat/2)**2 + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dLon/2)**2
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))

def get_queue_url_sync() -> str:
    # botocore 계측이 sub-span을 자동으로 생성함
    resp = ssm_client.get_parameter(
        Name="/finguard/dev/finance/trade_queue_host",
        WithDecryption=False
    )
    return resp["Parameter"]["Value"]


# ---------------- 스레드풀에서 돌릴 "순수 DB 함수"들 ----------------

def _check_fraud(conn, counter_account: str) -> bool:
    with conn.cursor() as cur:
        cur.execute("SELECT 1 FROM fraud WHERE accountNumber=%s LIMIT 1", (counter_account,))
        return cur.fetchone() is not None

def _select_my_account(conn, my_account_number: str):
    with conn.cursor() as cur:
        cur.execute("SELECT account_id, balance FROM accounts WHERE accountNumber=%s", (my_account_number,))
        return cur.fetchone()

def _select_counter_account(conn, counter_account_number: str):
    with conn.cursor() as cur:
        cur.execute("SELECT account_id FROM accounts WHERE accountNumber=%s", (counter_account_number,))
        return cur.fetchone()

def _get_last_tx_location(conn, account_id: str) -> Optional[Tuple[float, float]]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT ST_Y(transaction_gps) AS lat, ST_X(transaction_gps) AS lon
            FROM transactions
            WHERE account_id=%s AND type='debit'
            ORDER BY date DESC, time DESC
            LIMIT 1
            """,
            (account_id,)
        )
        row = cur.fetchone()
        return (row["lat"], row["lon"]) if row else None

def _get_home_location(conn, user_sub: str) -> Optional[Tuple[float, float]]:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT ST_Y(gps_location) AS lat, ST_X(gps_location) AS lon FROM users WHERE userSub=%s",
            (user_sub,)
        )
        row = cur.fetchone()
        return (row["lat"], row["lon"]) if row else None

def _has_repeat_retailer(conn, account_id: str, counter_account: str) -> bool:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT 1 FROM transactions WHERE account_id=%s AND counter_account=%s LIMIT 1",
            (account_id, counter_account)
        )
        return cur.fetchone() is not None

def _apply_transfer(conn, my_account_id: str, counter_account_id: str, amount: float,
                    description: str, gps_wkt: str) -> tuple[str, str]:
    cur = conn.cursor()
    try:
        conn.begin()
        now = datetime.utcnow()
        date = now.strftime("%Y-%m-%d")
        time_s = now.strftime("%H:%M:%S")
        debit_id = str(uuid.uuid4())
        credit_id = str(uuid.uuid4())

        # 조건부 차감
        cur.execute(
            "UPDATE accounts SET balance = balance - %s WHERE account_id = %s AND balance >= %s",
            (amount, my_account_id, amount)
        )
        if cur.rowcount != 1:
            conn.rollback()
            raise ValueError("INSUFFICIENT_BALANCE")

        # 상대 계좌 증액
        cur.execute(
            "UPDATE accounts SET balance = balance + %s WHERE account_id = %s",
            (amount, counter_account_id)
        )

        # 거래 내역 (debit)
        cur.execute(
            """
            INSERT INTO transactions
                (transaction_id, account_id, date, description, time, amount, type, transaction_gps, counter_account)
            VALUES
                (%s, %s, %s, %s, %s, %s, %s, ST_PointFromText(%s), %s)
            """,
            (debit_id, my_account_id, date, description, time_s, -amount, "debit", gps_wkt, counter_account_id)
        )

        # 거래 내역 (credit)
        cur.execute(
            """
            INSERT INTO transactions
                (transaction_id, account_id, date, description, time, amount, type, transaction_gps, counter_account)
            VALUES
                (%s, %s, %s, %s, %s, %s, %s, ST_PointFromText(%s), %s)
            """,
            (credit_id, counter_account_id, date, "입금", time_s, amount, "credit", gps_wkt, my_account_id)
        )

        conn.commit()
        return debit_id, credit_id
    except Exception:
        try:
            conn.rollback()
        except Exception:
            pass
        raise

def _load_median_heaps(conn, account_number: str) -> tuple[MinHeap, MaxHeap]:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT minHeap, maxHeap FROM median_prices WHERE account_number=%s",
            (account_number,)
        )
        row = cur.fetchone()
        min_heap = MinHeap(json.loads(row["minHeap"])) if (row and row["minHeap"]) else MinHeap([])
        max_heap = MaxHeap(json.loads(row["maxHeap"])) if (row and row["maxHeap"]) else MaxHeap([])
        return min_heap, max_heap

def _save_median_heaps(conn, account_number: str, min_heap: MinHeap, max_heap: MaxHeap):
    with conn.cursor() as cur:
        cur.execute(
            "REPLACE INTO median_prices (account_number, minHeap, maxHeap) VALUES (%s, %s, %s)",
            (account_number, json.dumps(min_heap.to_array()), json.dumps(max_heap.to_array()))
        )
    conn.commit()

# ---------------- 라우트 ----------------

@router.post("/transaction", response_model=TransactionSuccessResponse, status_code=201)
async def create_transaction(payload: TransactionRequest, request: Request):
    logging.info("⚙️Starting create transaction")
    logging.info(
        "create Transaction called : %s, %s, %s, %s, %s",
        payload.userSub, payload.my_account, payload.counter_account, payload.money, payload.location
    )
    conn = get_connection()
    try:
        # FastAPI 계측으로 inbound trace context는 자동 추출됨
        with tracer.start_as_current_span("txn.create") as root_span:
            root_span.set_attribute("http.route", "/transaction")
            root_span.set_attribute("http.method", "POST")
            root_span.set_attribute("user.sub", payload.userSub)

            # 0) 큐 URL (SSM)
            with tracer.start_as_current_span("ssm.get_queue_url"):
                logging.info("Fetching SQS queue URL from SSM ...")
                queue_url = get_queue_url_sync()
            
            
            with conn.cursor() as cur:  # 커서는 스레드만 접근
                # 1) 사기 계좌 체크
                with tracer.start_as_current_span("sql.check_fraud") as span:
                    logging.info("Checking for fraudulent counter account ...")
                    is_fraud = await anyio.to_thread.run_sync(_check_fraud, conn, payload.counter_account)
                    span.set_attribute("account.counter", payload.counter_account)
                    if is_fraud:
                        logging.warning("🚨Fraudulent account detected")
                        raise HTTPException(status_code=403, detail={"error": "FraudulentAccount", "message": "사기 계좌로 송금할 수 없습니다."})
                    logging.info("No fraud detected for counter account.")

                # 2) 내 계좌
                with tracer.start_as_current_span("sql.select_my_account") as span:
                    logging.info("Fetching my account information ...")
                    my_account = await anyio.to_thread.run_sync(_select_my_account, conn, payload.my_account)
                    span.set_attribute("account.my", payload.my_account)
                    if not my_account:
                        logging.warning("🚨My account not found")
                        raise HTTPException(status_code=404, detail={"error":"MyAccountNotFound","message":"내 계좌를 찾을 수 없습니다."})
                    if my_account["balance"] < float(payload.money):
                        logging.warning("🚨Insufficient balance for the transaction")
                        raise HTTPException(status_code=400, detail={"error": "InsufficientBalance", "message": "잔고가 부족합니다."})
                    logging.info(f"My account {payload.my_account} has sufficient balance: {my_account['balance']}")
                
                # 3) 상대 계좌
                with tracer.start_as_current_span("sql.select_counter_account") as span:
                    logging.info("Fetching counter account information ...")
                    counter_account = await anyio.to_thread.run_sync(_select_counter_account, conn, payload.counter_account)
                    if not counter_account:
                        logging.warning("🚨Counter account not found")
                        raise HTTPException(status_code=404, detail={"error": "CounterAccountNotFound", "message": "해당 계좌를 찾을 수 없습니다."})
                    logging.info(f"Counter account {payload.counter_account} found with ID {counter_account['account_id']}")

                # 4) 마지막 거래 위치
                with tracer.start_as_current_span("sql.last_tx_location"):
                    logging.info("Fetching last transaction location ...")
                    gps_last = await anyio.to_thread.run_sync(_get_last_tx_location, conn, my_account["account_id"])

                # 5) 홈 좌표
                with tracer.start_as_current_span("sql.get_home_location"):
                    logging.info("Fetching user's home location ...")
                    gps_home = await anyio.to_thread.run_sync(_get_home_location, conn, payload.userSub)
                    if not gps_home:
                        logging.warning("🚨User home location not found")
                        raise HTTPException(status_code=404, detail={"error":"UserNotFound","message":"유저 정보를 찾을 수 없습니다."})
                    logging.info(f"User's home location: {gps_home}")

                # 6) 특징 계산 (비-DB)
                with tracer.start_as_current_span("biz.compute_features") as span:
                    logging.info("Computing transaction features ...")
                    distance_from_home = haversine_distance(gps_home, payload.location)
                    distance_from_last = haversine_distance(gps_last, payload.location) if gps_last else 0.0
                    repeat_retailer = await anyio.to_thread.run_sync(
                        _has_repeat_retailer, conn, my_account["account_id"], payload.counter_account
                    )
                    repeat_retailer = 1.0 if repeat_retailer else 0.0
                    used_chip = int(payload.used_card) if payload.used_card else 0
                    gps_wkt = f"POINT({payload.location[1]} {payload.location[0]})"
                    span.set_attribute("feature.distance_from_home", distance_from_home)
                    span.set_attribute("feature.distance_from_last", distance_from_last)
                    span.set_attribute("feature.repeat_retailer", repeat_retailer)
                    span.set_attribute("feature.used_chip", used_chip)
                    logging.info(
                        f"Features computed: distance_from_home={distance_from_home}, "
                        f"distance_from_last={distance_from_last}, repeat_retailer={repeat_retailer}, used_chip={used_chip}"
                    )

                # 7) 송금 적용 (원자성)
                with tracer.start_as_current_span("sql.apply_transfer"):
                    logging.info("Applying transfer between accounts ...")
                    try:
                        debit_id, credit_id = await anyio.to_thread.run_sync(
                            _apply_transfer,
                            conn,
                            my_account["account_id"],
                            counter_account["account_id"],
                            float(payload.money),
                            (payload.description or "출금"),
                            gps_wkt
                        )
                    except ValueError as ve:
                        if str(ve) == "INSUFFICIENT_BALANCE":
                            logging.warning("🚨Insufficient balance detected during transfer application")
                            raise HTTPException(status_code=400, detail={"error":"InsufficientBalance","message":"잔고가 부족합니다."})
                        logging.exception("🚨Error applying transfer")
                        raise
                    logging.info(f"Transfer applied successfully: debit_id={debit_id}, credit_id={credit_id}")

                # 8) 중앙값 갱신
                with tracer.start_as_current_span("sql.update_median"):
                    logging.info("Updating median heaps for transaction amounts ...")
                    min_heap, max_heap = await anyio.to_thread.run_sync(_load_median_heaps, conn, payload.my_account)

                    if max_heap.size() and min_heap.size() and max_heap.size() == min_heap.size():
                        median_before = (max_heap.peek() + min_heap.peek()) / 2
                    elif max_heap.size():
                        median_before = max_heap.peek()
                    else:
                        median_before = 0.0

                    ratio_to_median = float(payload.money) / median_before if median_before else 1.0

                    if max_heap.size() == 0 or float(payload.money) < max_heap.peek():
                        max_heap.push(float(payload.money))
                    else:
                        min_heap.push(float(payload.money))
                    if max_heap.size() > min_heap.size() + 1:
                        min_heap.push(max_heap.pop())
                    elif min_heap.size() > max_heap.size():
                        max_heap.push(min_heap.pop())

                    await anyio.to_thread.run_sync(_save_median_heaps, conn, payload.my_account, min_heap, max_heap)
                    logging.info(f"Median heaps updated. New median: { (max_heap.peek() if max_heap.size() > min_heap.size() else (max_heap.peek() + min_heap.peek()) / 2) }")

                # 9) SQS 전송
                with tracer.start_as_current_span("sqs.send_message") as span:
                    logging.info("💬Sending transaction data to SQS ...")
                    message = {
                        "userSub": payload.userSub,
                        "features": [distance_from_home, distance_from_last, ratio_to_median, repeat_retailer, used_chip],
                    }
                    dedup_id = f"{int(datetime.utcnow().timestamp() * 1000)}-{uuid.uuid4()}"
                    # botocore 계측 + OTEL_PROPAGATORS=xray 설정으로
                    # 전파 헤더는 MessageAttributes에 자동 주입된다.
                    sqs_client.send_message(
                        QueueUrl=queue_url,
                        MessageBody=json.dumps(message),
                        MessageGroupId="trade-group",             # FIFO인 경우
                        MessageDeduplicationId=dedup_id,          # FIFO인 경우
                        # MessageAttributes는 수동 주입하지 않음(자동 주입)
                    )
                    logging.info("Message sent to SQS successfully.")

        # 응답
        return TransactionSuccessResponse(
            message="Transfer completed",
            transactions=TransactionMap(
                debit=TransactionDetail(
                    transactionId=debit_id,
                    accountId=my_account["account_id"],
                    amount=-float(payload.money),
                ),
                credit=TransactionDetail(
                    transactionId=credit_id,
                    accountId=counter_account["account_id"],
                    amount=float(payload.money),
                ),
            ),
        )

    except HTTPException:
        logging.exception("🚨create_transaction failed with HTTPException")
        raise
    except Exception:
        logging.exception("🚨create_transaction failed")
        try:
            conn.rollback()
        except Exception:
            pass
        raise HTTPException(status_code=500, detail={"error": "InternalError", "message": "🚨송금 처리 중 에러 발생"})
    finally:
        try:
            if conn:
                conn.close()
        except Exception:
            pass
