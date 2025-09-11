# api/account.py
import time
import random
import logging
from uuid import uuid4
from fastapi import APIRouter, HTTPException
from fastapi.responses import Response
import anyio

# 변경
from datetime import datetime, date, time as dtime

from models.schema import (
    CreateAccountRequest,
    AccountResponse,
    AccountCreateResult,
    AccountDetailResponse,
    GetAccountListRequest,
    GetAccountListResponse,
    TransactionItem,
)
from db.rds import get_connection
from db.dynamo import store_fcm_token

# OpenTelemetry
from opentelemetry import trace
from opentelemetry.trace import Status, StatusCode


# Time for transaction
def _time_hhmmss(val):
    """HH:MM:SS로 반환 (time/datetime/str 모두 허용)"""
    if not val:
        return None
    if isinstance(val, (datetime, dtime)):
        return val.strftime("%H:%M:%S")
    if isinstance(val, str):
        s = val.strip()
        # ISO 형식 우선 시도
        try:
            return datetime.fromisoformat(s.replace("Z", "+00:00")).strftime("%H:%M:%S")
        except ValueError:
            pass
        # 'HH:MM' 또는 'HH:MM:SS' 직접 파싱
        try:
            h, m, *rest = s.split(":")
            sec = int(rest[0]) if rest else 0
            return f"{int(h):02d}:{int(m):02d}:{sec:02d}"
        except Exception:
            return None
    return None


# ── Prometheus
from prometheus_client import Counter, Histogram, generate_latest, CONTENT_TYPE_LATEST

router = APIRouter()
tracer = trace.get_tracer(
    "account-service"
)  # service.name은 OTEL_RESOURCE_ATTRIBUTES로도 들어감

# ─────────────── Prometheus metrics ───────────────
API_REQS = Counter(
    "account_api_requests_total", "API calls", ["route", "method", "status"]
)
API_LATENCY = Histogram(
    "account_api_latency_seconds", "API latency seconds", ["route", "method"]
)


def _observe(route: str, method: str, status: str, start_ts: float):
    API_REQS.labels(route=route, method=method, status=status).inc()
    API_LATENCY.labels(route=route, method=method).observe(
        time.perf_counter() - start_ts
    )


# ─────────────── helpers ───────────────
def generate_account_number() -> str:
    part1 = str(random.randint(100, 999))
    part2 = str(random.randint(100, 999))
    part3 = str(random.randint(10000, 99999))
    return f"{part1}-{part2}-{part3}"


def _check_unique_account(conn, acc_num: str) -> bool:
    query = "SELECT 1 FROM accounts WHERE accountNumber = %s"
    with conn.cursor() as cursor:
        cursor.execute(query, (acc_num,))
        return cursor.fetchone() is None


def _generate_unique_account_number(conn) -> str:
    while True:
        acc_num = generate_account_number()
        if _check_unique_account(conn, acc_num):
            return acc_num


def _insert_account(conn, params):
    with conn.cursor() as cursor:
        cursor.execute(
            """
            INSERT INTO accounts (
                account_id, userSub, accountName, accountNumber, bankName, balance
            ) VALUES (%s, %s, %s, %s, %s, 0)
            """,
            params,
        )
        conn.commit()


def _select_account(conn, account_id: str):
    with conn.cursor() as cursor:
        cursor.execute(
            """
            SELECT account_id, accountName, accountNumber, balance, bankName
            FROM accounts WHERE account_id = %s
            """,
            (account_id,),
        )
        return cursor.fetchone()


def _select_transactions(conn, account_id: str):
    with conn.cursor() as cursor:
        cursor.execute(
            """
            SELECT transaction_id, date, time, description, amount, type
            FROM transactions WHERE account_id = %s
            ORDER BY date DESC, time DESC
            """,
            (account_id,),
        )
        return cursor.fetchall()


def _select_accounts_by_user(conn, user_sub: str):
    with conn.cursor() as cursor:
        cursor.execute(
            """
            SELECT account_id, accountName, accountNumber, balance, bankName
            FROM accounts WHERE userSub = %s
            """,
            (user_sub,),
        )
        return cursor.fetchall()


# ─────────────── metrics & health ───────────────
@router.get("/metrics")
def metrics():
    # Prometheus receiver(ADOT)가 스크레이프하는 엔드포인트
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)


@router.get("/health")
def health():
    return {"status": "ok"}


# ─────────────── routes ───────────────
@router.post("/accounts/create", response_model=AccountCreateResult)
async def create_account(payload: CreateAccountRequest):
    route, method = "/accounts/create", "POST"
    t0 = time.perf_counter()
    logging.info("⚙️Starting createAccount API")
    conn = get_connection()
    try:
        with tracer.start_as_current_span("biz.generate_unique_number") as span:
            try:
                logging.info("Generating unique account number ...")
                account_number = await anyio.to_thread.run_sync(
                    _generate_unique_account_number, conn
                )
                span.set_attribute("app.account.number", account_number)
            except Exception as e:
                logging.exception("🚨Error generating unique account number")
                span.record_exception(e)
                span.set_status(Status(StatusCode.ERROR, str(e)))
                raise
        logging.info(f"Generated account number: {account_number}")

        account_id = str(uuid4())
        with tracer.start_as_current_span("sql.insert_account") as span:
            logging.info("Inserting new account into database ... ")
            span.set_attribute("db.system", "mysql")
            span.set_attribute("db.statement", "INSERT INTO accounts(...) VALUES(...)")
            try:
                await anyio.to_thread.run_sync(
                    _insert_account,
                    conn,
                    (
                        account_id,
                        payload.userSub,
                        payload.accountName,
                        account_number,
                        payload.bankName,
                    ),
                )
            except Exception as e:
                span.record_exception(e)
                span.set_status(Status(StatusCode.ERROR, str(e)))
                raise

        resp = AccountCreateResult(
            message="Account created successfully",
            account=AccountResponse(
                accountId=account_id,
                accountName=payload.accountName,
                accountNumber=account_number,
                balance=0,
            ),
        )
        _observe(route, method, "200", t0)
        return resp
    except Exception as e:
        logging.exception("createAccount failed")
        _observe(route, method, "error", t0)
        raise HTTPException(status_code=400, detail="create_account ERROR : " + str(e))
    finally:
        if conn:
            conn.close()


@router.get("/accounts/{account_id}", response_model=AccountDetailResponse)
async def get_account(account_id: str):
    route, method = "/accounts/{account_id}", "GET"
    t0 = time.perf_counter()
    logging.info("⚙️Starting getAccount API")
    conn = get_connection()
    try:
        with tracer.start_as_current_span("sql.select_account") as span:
            logging.info("Fetching account details from database ...")
            span.set_attribute("db.system", "mysql")
            span.set_attribute(
                "db.statement", "SELECT ... FROM accounts WHERE account_id = ?"
            )
            span.set_attribute("db.param.account_id", account_id)
            try:
                acc = await anyio.to_thread.run_sync(_select_account, conn, account_id)
                if not acc:
                    logging.warning(f"Account not found: {account_id}")
                    _observe(route, method, "404", t0)
                    raise HTTPException(status_code=404, detail="Account not found")
            except Exception as e:
                if not isinstance(e, HTTPException):
                    logging.exception("🚨Error fetching account details")
                    span.record_exception(e)
                    span.set_status(Status(StatusCode.ERROR, str(e)))
                raise

        with tracer.start_as_current_span("sql.select_transactions") as span:
            logging.info("Fetching transactions from database ...")
            span.set_attribute("db.system", "mysql")
            span.set_attribute(
                "db.statement", "SELECT ... FROM transactions WHERE account_id = ?"
            )
            try:
                txs = await anyio.to_thread.run_sync(
                    _select_transactions, conn, account_id
                )
            except Exception as e:
                logging.exception("🚨Error fetching transactions")
                span.record_exception(e)
                span.set_status(Status(StatusCode.ERROR, str(e)))
                raise

        transactions = [
            TransactionItem(
                id=row["transaction_id"],
                date=(row.get("date") or "").strip() or None,  
                time=(row.get("time") or "").strip() or None,  
                description=row.get("description"),
                amount=row.get("amount"),
                type="credit" if row.get("type") == "입금" else "debit",
            )
            for row in txs
        ]

        resp = AccountDetailResponse(
            accountId=acc["account_id"],
            accountName=acc["accountName"],
            accountNumber=acc["accountNumber"],
            bankName=acc["bankName"],
            balance=acc["balance"],
            transactions=transactions,
        )
        _observe(route, method, "200", t0)
        return resp
    except HTTPException as e:
        _observe(route, method, str(e.status_code), t0)
        raise
    except Exception as e:
        logging.exception("🚨getAccount failed")
        _observe(route, method, "error", t0)
        raise HTTPException(status_code=400, detail="get_account ERROR : " + str(e))
    finally:
        if conn:
            conn.close()


@router.post("/accounts/financial", response_model=GetAccountListResponse)
async def get_account_list(payload: GetAccountListRequest):
    route, method = "/accounts/financial", "POST"
    t0 = time.perf_counter()
    logging.info("⚙️Starting getAccountList API")
    conn = get_connection()
    try:
        with tracer.start_as_current_span("ddb.store_fcm_token") as span:
            try:
                logging.info("Storing FCM token in DynamoDB ...")
                store_fcm_token(payload.sub, payload.fcmToken)
                span.set_attribute("app.user.sub", payload.sub)
            except Exception as e:
                logging.exception("🚨Error storing FCM token in DynamoDB")
                span.record_exception(e)
                span.set_status(Status(StatusCode.ERROR, str(e)))
                raise

        with tracer.start_as_current_span("sql.select_accounts_by_userSub") as span:
            logging.info("Fetching accounts from database ...")
            span.set_attribute("db.system", "mysql")
            span.set_attribute(
                "db.statement", "SELECT ... FROM accounts WHERE userSub = ?"
            )
            try:
                accounts = await anyio.to_thread.run_sync(
                    _select_accounts_by_user, conn, payload.sub
                )
            except Exception as e:
                logging.exception("🚨Error fetching accounts")
                span.record_exception(e)
                span.set_status(Status(StatusCode.ERROR, str(e)))
                raise

        resp = GetAccountListResponse(
            sub=payload.sub,
            accounts=[
                AccountResponse(
                    accountId=row["account_id"],
                    accountName=row["accountName"],
                    accountNumber=row["accountNumber"],
                    bankName=row["bankName"],
                    balance=row["balance"],
                )
                for row in accounts
            ],
        )
        _observe(route, method, "200", t0)
        return resp
    except Exception as e:
        logging.exception("🚨getAccountList failed")
        _observe(route, method, "error", t0)
        raise HTTPException(
            status_code=400, detail="get_account_list ERROR : " + str(e)
        )
    finally:
        if conn:
            conn.close()
