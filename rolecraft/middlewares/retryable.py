import dataclasses
import random
from collections.abc import Callable, Sequence
from typing import TypedDict, Unpack

from rolecraft.queue import Message, MessageQueue
from rolecraft.role_lib import ActionError

from .base_middleware import BaseMiddleware


class Retryable(BaseMiddleware):
    _BASE_BACKOFF_MILLIS = 5 * 60 * 1000
    _MAX_BACKOFF_MILLIS = 366 * 24 * 60 * 60 * 1000

    class Options(TypedDict, total=False):
        max_retries: int
        base_backoff_millis: int
        max_backoff_millis: int
        exponential_factor: float  # Exponential factor for backoff calculation
        jitter_range: float  # Random jitter range as a percentage of the base backoff time

        should_retry: Callable[[Exception, int, Message], bool] | None
        raises: Sequence[type[Exception]] | type[Exception]

    @dataclasses.dataclass
    class Meta(BaseMiddleware.Meta):
        retries: int = 0

    def __init__(
        self, queue: MessageQueue | None = None, **options: Unpack[Options]
    ) -> None:
        self.max_retries = options.get("max_retries", 3)
        self.base_backoff_millis = options.get(
            "base_backoff_millis", self._BASE_BACKOFF_MILLIS
        )
        self.max_backoff_millis = options.get(
            "max_backoff_millis", self._MAX_BACKOFF_MILLIS
        )
        self.exponential_factor = options.get("exponential_factor", 2)
        self.jitter_range = options.get("jitter_range", 0.2)

        self.should_retry = options.get("should_retry")
        raises = options.get("raises") or ()
        self.raises = tuple(raises) if isinstance(raises, Sequence) else raises

        super().__init__(queue)

    @property
    def options(self):
        return self.Options(
            max_retries=self.max_retries,
            base_backoff_millis=self.base_backoff_millis,
            max_backoff_millis=self.max_backoff_millis,
            exponential_factor=self.exponential_factor,
            jitter_range=self.jitter_range,
            should_retry=self.should_retry,
            raises=self.raises,
        )

    def _should_retry(
        self, message: Message, exception: Exception, retry_attempt: int
    ) -> bool:
        if not isinstance(exception, ActionError):
            return False

        if self.raises and (
            isinstance(exception.__cause__, self.raises)
            or isinstance(exception, self.raises)
        ):
            return False

        if should_retry := self.should_retry:
            if exception.__cause__:
                assert isinstance(exception.__cause__, Exception)
            return should_retry(
                exception.__cause__ or exception, retry_attempt, message
            )

        return retry_attempt < self.max_retries

    def nack(self, message: Message, exception: Exception, **kwargs):
        retries = self.Meta.create_from(message.meta).retries

        if not self._should_retry(message, exception, retries):
            return self._guarded_queue.nack(
                message, exception=exception, **kwargs
            )

        delay_millis = int(self._compute_delay_millis(retries))
        self._guarded_queue.retry(
            message, delay_millis=delay_millis, exception=exception
        )
        return

    def _compute_delay_millis(self, retry_attempt: int) -> float:
        if retry_attempt == 0:
            return min(self.base_backoff_millis, self.max_backoff_millis)

        backoff_time: float = self.base_backoff_millis * (
            self.exponential_factor**retry_attempt
        )

        jitter = random.uniform(-self.jitter_range, self.jitter_range)
        backoff_time += self.base_backoff_millis * jitter

        return min(backoff_time, self.max_backoff_millis)
