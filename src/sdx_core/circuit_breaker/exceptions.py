"""Circuit breaker exceptions.

Callers can distinguish between:
  - A call being rejected because the circuit is open.
"""


class CircuitBreakerError(Exception):
    """Base exception for the circuit breaker package."""


class CircuitOpenError(CircuitBreakerError):
    """Raised when a call is rejected because the circuit is open.

    Attributes:
        breaker_name: Name of the breaker rejecting the call.
        retry_after: Seconds until a half-open probe may be attempted.
    """

    def __init__(self, breaker_name: str, retry_after: float) -> None:
        """Initialize a circuit-open exception payload.

        Args:
            breaker_name: Breaker rejecting the call.
            retry_after: Seconds until the next probe window opens.
        """
        self.breaker_name = breaker_name
        self.retry_after = retry_after
        super().__init__(f"circuit_open: {breaker_name} retry_after={retry_after:g}s")
