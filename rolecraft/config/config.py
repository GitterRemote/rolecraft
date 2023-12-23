from typing import Self
import dataclasses
from rolecraft import middleware as _middleware
from rolecraft.middleware import Middleware, MiddlewareList
from rolecraft.encoder import Encoder
from rolecraft import encoder as _encoder_mod


@dataclasses.dataclass
class Config:
    middlewares: list[Middleware] | MiddlewareList | None

    consumer_wait_time_seconds: int | None
    encoder: Encoder | None

    @classmethod
    def default(cls) -> Self:
        return cls(
            middlewares=MiddlewareList([_middleware.Retryable()]),
            consumer_wait_time_seconds=10 * 60,
            encoder=_encoder_mod.DefaultBytesEncoder(),
        )

    def merge_from(self, config: Self) -> Self:
        for field in dataclasses.fields(self):
            if getattr(self, field.name) is None:
                value = getattr(config, field.name)
                if value is not None:
                    setattr(self, field.name, value)
        return self
