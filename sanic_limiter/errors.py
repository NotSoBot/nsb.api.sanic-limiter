"""
errors and exceptions
"""
from typing import Optional

from limits import RateLimitItem
from sanic.exceptions import SanicException


class RateLimitExceeded(SanicException):
    status_code = 429

    def __init__(
        self,
        message: Optional[str] = None,
        failed_limits: Optional[list[tuple[str, str, RateLimitItem]]] = None,
    ):
        message = message or 'too many requests'
        super().__init__(message)
        self.failed_limits = failed_limits or []
