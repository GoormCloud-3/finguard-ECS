# models/schema.py
from pydantic import BaseModel
from typing import List, Optional
from datetime import date, time

class CreateAccountRequest(BaseModel):
    userSub: str
    accountName: str
    bankName: str


class TransactionItem(BaseModel):
    id: str
    date: Optional[date]   # <-- str 대신 date
    time: Optional[time]   # <-- str 대신 time
    description: str
    amount: float
    type: str  # "credit" | "debit"


class AccountResponse(BaseModel):
    accountId: str
    accountName: str
    accountNumber: str
    balance: float


class AccountDetailResponse(AccountResponse):
    transactions: List[TransactionItem]


class GetAccountListRequest(BaseModel):
    sub: str
    fcmToken: List[str]  # 배열로 받으려면 List[str]로 정의


class GetAccountListResponse(BaseModel):
    sub: str
    accounts: List[AccountResponse]


class AccountCreateResult(BaseModel):
    message: str
    account: AccountResponse
