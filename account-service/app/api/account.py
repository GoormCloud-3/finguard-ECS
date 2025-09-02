# api/account.py
import random
import logging
from uuid import uuid4
from fastapi import APIRouter, HTTPException
import anyio

from models.schema import (
    CreateAccountRequest, AccountResponse, AccountCreateResult,
    AccountDetailResponse, GetAccountListRequest, GetAccountListResponse, TransactionItem
)
from db.rds import get_connection
from db.dynamo import store_fcm_token

# 🔁 X-Ray SDK 제거하고 OpenTelemetry 사용
from opentelemetry import trace
from opentelemetry.trace import Status, StatusCode

router = APIRouter()
tracer = trace.get_tracer("account-service")  # 서비스명은 OTEL_RESOURCE_ATTRIBUTES로도 들어감


def generate_account_number() -> str:
    part1 = str(random.randint(100, 999))
    part2 = str(random.randint(100, 999))
    part3 = str(random.randint(10000, 99999))
    return f"{part1}-{part2}-{part3}"


# ----- 스레드풀에서 실행될 순수 DB/비즈 함수 -----

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
        cursor.execute("""
            INSERT INTO accounts (
                account_id, userSub, accountName, accountNumber, bankName, balance
            ) VALUES (%s, %s, %s, %s, %s, 0)
        """, params)
        conn.commit()

def _select_account(conn, account_id: str):
    with conn.cursor() as cursor:
        cursor.execute("""
            SELECT account_id, accountName, accountNumber, balance, bankName
            FROM accounts WHERE account_id = %s
        """, (account_id,))
        return cursor.fetchone()

def _select_transactions(conn, account_id: str):
    with conn.cursor() as cursor:
        cursor.execute("""
            SELECT transaction_id, date, time, description, amount, type
            FROM transactions WHERE account_id = %s
            ORDER BY date DESC, time DESC
        """, (account_id,))
        return cursor.fetchall()

def _select_accounts_by_user(conn, user_sub: str):
    with conn.cursor() as cursor:
        cursor.execute("""
            SELECT account_id, accountName, accountNumber, balance, bankName
            FROM accounts WHERE userSub = %s
        """, (user_sub,))
        return cursor.fetchall()


# ---------------------------- 라우트 ----------------------------

@router.post("/accounts/create", response_model=AccountCreateResult)
async def create_account(payload: CreateAccountRequest):
    logging.info("Starting createAccount API")
    conn = get_connection()
    try:
        # 비즈: 고유 계좌번호 생성
        with tracer.start_as_current_span("biz.generate_unique_number") as span:
            try:
                account_number = await anyio.to_thread.run_sync(_generate_unique_account_number, conn)
                span.set_attribute("app.account.number", account_number)
            except Exception as e:
                span.record_exception(e)
                span.set_status(Status(StatusCode.ERROR, str(e)))
                raise
        # DB INSERT
        account_id = str(uuid4())
        with tracer.start_as_current_span("sql.insert_account") as span:
            span.set_attribute("db.system", "mysql")  # 사용 DB에 맞게
            span.set_attribute("db.statement", "INSERT INTO accounts(...) VALUES(...)")
            try:
                await anyio.to_thread.run_sync(
                    _insert_account,
                    conn,
                    (account_id, payload.userSub, payload.accountName, account_number, payload.bankName)
                )
            except Exception as e:
                span.record_exception(e)
                span.set_status(Status(StatusCode.ERROR, str(e)))
                raise

        return AccountCreateResult(
            message="Account created successfully",
            account=AccountResponse(
                accountId=account_id,
                accountName=payload.accountName,
                accountNumber=account_number,
                balance=0
            )
        )
    except Exception as e:
        logging.exception("createAccount failed")
        raise HTTPException(status_code=400, detail="create_account ERROR : " + str(e))
    finally:
        if conn:
            conn.close()


@router.get("/accounts/{account_id}", response_model=AccountDetailResponse)
async def get_account(account_id: str):
    logging.info("Starting getAccount API")
    conn = get_connection()
    try:
        # 계좌 상세
        with tracer.start_as_current_span("sql.select_account") as span:
            span.set_attribute("db.system", "mysql")
            span.set_attribute("db.statement", "SELECT ... FROM accounts WHERE account_id = ?")
            span.set_attribute("db.param.account_id", account_id)
            try:
                acc = await anyio.to_thread.run_sync(_select_account, conn, account_id)
                if not acc:
                    raise HTTPException(status_code=404, detail="Account not found")
            except Exception as e:
                if not isinstance(e, HTTPException):
                    span.record_exception(e)
                    span.set_status(Status(StatusCode.ERROR, str(e)))
                raise

        # 거래 목록
        with tracer.start_as_current_span("sql.select_transactions") as span:
            span.set_attribute("db.system", "mysql")
            span.set_attribute("db.statement", "SELECT ... FROM transactions WHERE account_id = ?")
            try:
                txs = await anyio.to_thread.run_sync(_select_transactions, conn, account_id)
            except Exception as e:
                span.record_exception(e)
                span.set_status(Status(StatusCode.ERROR, str(e)))
                raise

        transactions = [
            TransactionItem(
                id=row["transaction_id"],
                date=row["date"].isoformat() if row["date"] else None,
                time=row["time"].strftime("%H:%M") if row["time"] else None,
                description=row["description"],
                amount=row["amount"],
                type="credit" if row["type"] == "입금" else "debit"
            ) for row in txs
        ]

        return AccountDetailResponse(
            accountId=acc["account_id"],
            accountName=acc["accountName"],
            accountNumber=acc["accountNumber"],
            bankName=acc["bankName"],
            balance=acc["balance"],
            transactions=transactions
        )
    except HTTPException:
        raise
    except Exception as e:
        logging.exception("getAccount failed")
        raise HTTPException(status_code=400, detail="get_account ERROR : " + str(e))
    finally:
        if conn:
            conn.close()


@router.post("/accounts/financial", response_model=GetAccountListResponse)
async def get_account_list(payload: GetAccountListRequest):
    logging.info("Starting getAccountList API")
    conn = get_connection()
    try:
        # DynamoDB 호출 (boto3 자동계측 쓰면 span 자동 생성됨. 수동으로도 감싸두자.)
        with tracer.start_as_current_span("ddb.store_fcm_token") as span:
            try:
                store_fcm_token(payload.sub, payload.fcmToken)
                span.set_attribute("app.user.sub", payload.sub)
            except Exception as e:
                span.record_exception(e)
                span.set_status(Status(StatusCode.ERROR, str(e)))
                raise

        # 계좌 목록 조회
        with tracer.start_as_current_span("sql.select_accounts_by_userSub") as span:
            span.set_attribute("db.system", "mysql")
            span.set_attribute("db.statement", "SELECT ... FROM accounts WHERE userSub = ?")
            try:
                accounts = await anyio.to_thread.run_sync(_select_accounts_by_user, conn, payload.sub)
            except Exception as e:
                span.record_exception(e)
                span.set_status(Status(StatusCode.ERROR, str(e)))
                raise

        return GetAccountListResponse(
            sub=payload.sub,
            accounts=[
                AccountResponse(
                    accountId=row["account_id"],
                    accountName=row["accountName"],
                    accountNumber=row["accountNumber"],
                    bankName=row["bankName"],
                    balance=row["balance"]
                ) for row in accounts
            ]
        )
    except Exception as e:
        logging.exception("getAccountList failed")
        raise HTTPException(status_code=400, detail="get_account_list ERROR : " + str(e))
    finally:
        if conn:
            conn.close()
