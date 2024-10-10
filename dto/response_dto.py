from dataclasses import dataclass
from typing import TypeVar, Optional

T = TypeVar("T")

@dataclass
class ResponseDto:
    message: Optional[str] = None
    error: Optional[str] = None
    body: Optional[T] = None