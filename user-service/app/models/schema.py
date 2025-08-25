# models/schema.py
from pydantic import BaseModel
from typing import List, Optional

class UserResponse(BaseModel):
    message: str
    userSub: str

class UserRequest(BaseModel):
    userSub: str
    gps_location: List[float]  # [latitude, longitude]