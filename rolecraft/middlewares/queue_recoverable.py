import functools
import logging
from collections.abc import Callable
from typing import TypedDict, Unpack

from rolecraft.broker import RecoverableError
from rolecraft.queue import MessageQueue

from .base_middleware import BaseMiddleware

logger = logging.getLogger(__name__)


class QueueRecoverable(BaseMiddleware):
    """Retries recoverable errors from the queue methods, such as receive, requeue, etc.

    It is distinct from Retryable in that Retryable primarily handles Role ActionError, which is raised from user functions, but QueueRecoverable is responsible for managing queue errors.
    """

    class Options(TypedDict, total=False):
        queue_retries: int

    def __init__(
        self, queue: MessageQueue | None = None, **options: Unpack[Options]
    ) -> None:
        super().__init__(queue)
        self.retries = options.get("queue_retries", 3)

    @property
    def options(self):
        return self.Options(queue_retries=self.retries)

    def __getattr__(self, name: str):
        attr = super().__getattr__(name)
        if callable(attr):
            return self._make_recoverale(attr)
        return attr

    def _make_recoverale[**P, R](self, fn: Callable[P, R]) -> Callable[P, R]:
        @functools.wraps(fn)
        def wrapper(*args: P.args, **kwds: P.kwargs) -> R:
            tried = 0
            while True:
                try:
                    return fn(*args, **kwds)
                except RecoverableError as exc:
                    tried += 1
                    if tried > self.retries:
                        raise
                    logger.error(
                        f"{fn.__name__.upper()} error, retrying for the %i time",
                        tried,
                        exc_info=exc,
                    )

        return wrapper
