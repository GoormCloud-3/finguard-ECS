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
from aws_xray_sdk.core import xray_recorder

router = APIRouter()


def generate_account_number() -> str:
    part1 = str(random.randint(100, 999))
    part2 = str(random.randint(100, 999))
    part3 = str(random.randint(10000, 99999))
    return f"{part1}-{part2}-{part3}"


# ----- 스레드풀에서 실행될 순수 DB/비즈 함수들 (X-Ray 호출 금지) -----

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
    logging.info(f"🔍 Creating account for userSub: {payload.userSub}")
    conn = get_connection()
    try:
        # 비즈: 고유 계좌번호 생성
        logging.info("Generating unique account number")
        with xray_recorder.in_subsegment("biz:generate_unique_number"):
            account_number = await anyio.to_thread.run_sync(_generate_unique_account_number, conn)
            logging.info(f"✅ Unique account number generated: {account_number}")
        logging.info("Generating unique account number completed")

        # DB INSERT
        account_id = str(uuid4())
        logging.info(f"💾 Inserting account into DB: {account_id}, {payload.userSub}, {payload.accountName}, {account_number}, {payload.bankName}")
        with xray_recorder.in_subsegment("sql:insert_account"):
            await anyio.to_thread.run_sync(
                _insert_account,
                conn,
                (account_id, payload.userSub, payload.accountName, account_number, payload.bankName)
            )
        logging.info(f"✅ Account created successfully: {account_id}, {payload.userSub}, {payload.accountName}, {account_number}, {payload.bankName}")

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
        logging.error(f"❌ Error during createAccount API : {e}")
        raise HTTPException(status_code=400, detail="create_account ERROR : " + str(e))
    finally:
        if conn:
            logging.info("🔚 Closing DB connection")
            conn.close()


@router.get("/accounts/{account_id}", response_model=AccountDetailResponse)
async def get_account(account_id: str):
    logging.info("Starting getAccount API")
    logging.info(f"🔍 Fetching account transactions for account_id: {account_id}")
    conn = get_connection()
    try:
        # 계좌 상세
        logging.info("Starting account detail query")
        with xray_recorder.in_subsegment("sql:select_account"):
            logging.info(f"💾 Querying account details for account_id: {account_id}")
            acc = await anyio.to_thread.run_sync(_select_account, conn, account_id)
            if not acc:
                logging.warning(f"⚠️ Account not found: {account_id}")
                raise HTTPException(status_code=404, detail="Account not found")
        logging.info(f"✅ Account details fetched: {acc}")
        logging.info("Account detail query completed")

        # 거래 목록
        logging.info("Starting transactions query")
        with xray_recorder.in_subsegment("sql:select_transactions"):
            logging.info(f"💾 Querying transactions for account_id: {account_id}")
            txs = await anyio.to_thread.run_sync(_select_transactions, conn, account_id)
        logging.info(f"✅ Transactions fetched: {len(txs)} items")
        logging.info("Transactions query completed")

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
    except Exception as e:
        logging.error(f"❌ Error during getAccount API : {e}")
        raise HTTPException(status_code=400, detail="get_account ERROR : " + str(e))
    finally:
        if conn:
            logging.info("🔚 Closing DB connection")
            conn.close()


@router.post("/accounts/financial", response_model=GetAccountListResponse)
async def get_account_list(payload: GetAccountListRequest):
    logging.info("Starting getAccountList API")
    logging.info(f"🔍 Fetching accounts for userSub: {payload.sub}")
    conn = get_connection()
    try:
        # DynamoDB(Firebase 토큰 저장) — boto3는 main.py에서 선택 패치되어 자동 subsegment 생성됨
        with xray_recorder.in_subsegment("ddb:store_fcm_token"):
            logging.info(f"💾 Storing FCM tokens for userSub: {payload.sub}")
            store_fcm_token(payload.sub, payload.fcmToken)
            logging.info(f"✅ FCM tokens stored for userSub: {payload.sub}")

        # 계좌 목록
        logging.info("Starting accounts query")
        with xray_recorder.in_subsegment("sql:select_accounts_by_userSub"):
            logging.info(f"💾 Querying accounts for userSub: {payload.sub}")
            accounts = await anyio.to_thread.run_sync(_select_accounts_by_user, conn, payload.sub)
        logging.info(f"✅ Accounts fetched: {len(accounts)} items")
        logging.info("Accounts query completed")

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
        logging.error(f"❌ Error during getAccountList API : {e}")
        raise HTTPException(status_code=400, detail="get_account_list ERROR : " + str(e))
    finally:
        if conn:
            logging.info("🔚 Closing DB connection")
            conn.close()
