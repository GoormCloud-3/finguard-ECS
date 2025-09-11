# models/schema.py
from pydantic import BaseModel
from typing import List, Optional, Union, Literal
from datetime import date as Date, time as Time


class CreateAccountRequest(BaseModel):
    userSub: str
    accountName: str
    bankName: str


class TransactionItem(BaseModel):
    id: str
    date: Optional[Union[str, Date]] = None
    time: Optional[Union[str, Time]] = None
    description: str
    amount: float
    type: Literal["credit", "debit"]


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
