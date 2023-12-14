"""
errors and exceptions
"""
from typing import Optional

from sanic.exceptions import SanicException

from extension import ExtLimit


class RateLimitExceeded(SanicException):
    status_code = 429

    def __init__(
        self,
        message: Optional[str] = None,
        failed_limits: Optional[list[ExtLimit]] = None,
    ):
        message = message or 'too many requests'
        super().__init__(message)
        self.failed_limits = failed_limits or []
