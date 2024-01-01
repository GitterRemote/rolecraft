import random
from collections.abc import Callable, Sequence
from typing import TypedDict, Unpack

from rolecraft.message import Message
from rolecraft.queue import MessageQueue

from .middleware import Middleware


class RetryableOptions(TypedDict, total=False):
    max_retries: int
    base_backoff_millis: int
    max_backoff_millis: int | None
    exponential_factor: float  # Exponential factor for backoff calculation
    jitter_range: float  # Random jitter range as a percentage of the base backoff time

    should_retry: Callable[[Message, Exception, int], bool] | None
    raises: Sequence[type[Exception]]


class Retryable(Middleware):
    _BASE_BACKOFF_MILLIS = 5 * 60 * 1000

    def __init__(
        self,
        queue: MessageQueue | None = None,
        **options: Unpack[RetryableOptions],
    ) -> None:
        self.max_retries = options.get("max_retries", 3)
        self.base_backoff_millis = options.get(
            "base_backoff_millis", self._BASE_BACKOFF_MILLIS
        )
        self.max_backoff_millis = options.get("max_backoff_millis")
        self.exponential_factor = options.get("exponential_factor", 2)
        self.jitter_range = options.get("jitter_range", 0.2)

        self.should_retry = options.get("should_retry")
        self.raises = tuple(options.get("raises") or ())

        super().__init__(queue)

    @property
    def options(self):
        return RetryableOptions(
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
        if self.raises and isinstance(exception, self.raises):
            return False

        if should_retry := self.should_retry:
            return should_retry(message, exception, retry_attempt)

        return retry_attempt < self.max_retries

    def nack(self, message: Message, exception: Exception, **kwargs):
        assert self.queue, "Middleware Retryable is not initialized"
        retries = message.meta.retries or 0

        if not self._should_retry(message, exception, retries):
            return self.queue.nack(message, exception=exception, **kwargs)

        delay_millis = int(self._compute_delay_millis(retries))
        return self.queue.retry(
            message, delay_millis=delay_millis, exception=exception
        )

    def _compute_delay_millis(self, retry_attempt: int):
        if retry_attempt == 0:
            return self.base_backoff_millis

        backoff_time: float = self.base_backoff_millis * (
            self.exponential_factor**retry_attempt
        )
        if (
            self.max_backoff_millis is not None
            and backoff_time > self.max_backoff_millis
        ):
            backoff_time = self.max_backoff_millis

        jitter = random.uniform(-self.jitter_range, self.jitter_range)
        backoff_time += self.base_backoff_millis * jitter

        return backoff_time
