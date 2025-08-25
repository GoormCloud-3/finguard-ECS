from pydantic import BaseModel
from typing import List, Optional


class TransactionRequest(BaseModel):
    userSub: str
    my_account: str
    counter_account: str
    money: float
    used_card: int
    location: List[float]
    description: Optional[str] = None


# 거래 내역 항목
class TransactionDetail(BaseModel):
    transactionId: str
    accountId: str
    amount: float


# 성공 응답 모델 (거래 내역을 리스트로)
class TransactionMap(BaseModel):
    debit: TransactionDetail
    credit: TransactionDetail

class TransactionSuccessResponse(BaseModel):
    message: str
    transactions: TransactionMap


# 실패 응답 모델
class TransactionErrorResponse(BaseModel):
    statusCode: int
    error: str
    message: str
