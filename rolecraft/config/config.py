from typing import Self
import dataclasses
from rolecraft import middleware as _middleware
from rolecraft import encoder as _encoder_mod


@dataclasses.dataclass
class Config:
    encoder: _encoder_mod.Encoder | None
    middlewares: list[
        _middleware.Middleware
    ] | _middleware.MiddlewareList | None

    consumer_wait_time_seconds: int | None

    @classmethod
    def default(cls) -> Self:
        return cls(
            encoder=None,
            middlewares=_middleware.MiddlewareList([_middleware.Retryable()]),
            consumer_wait_time_seconds=10 * 60,
        )
