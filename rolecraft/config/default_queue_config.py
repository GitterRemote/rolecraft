import dataclasses

from rolecraft import middlewares as _middleware
from rolecraft.broker import HeaderBytesRawMessage
from rolecraft.queue import HeaderBytesEncoder, IncompleteQueueConfig

from .middleware_list import MiddlewareList


@dataclasses.dataclass(kw_only=True, frozen=True)
class DefaultQueueConfig(IncompleteQueueConfig[HeaderBytesRawMessage]):
    middlewares: MiddlewareList = dataclasses.field(
        default_factory=lambda: MiddlewareList(
            [_middleware.QueueRecoverable(), _middleware.Retryable()]
        )
    )
    wait_time_seconds: int = 10 * 60
    encoder: HeaderBytesEncoder = HeaderBytesEncoder()
