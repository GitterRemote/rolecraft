import random
from typing import TypedDict, Unpack

from rolecraft.message import Message
from rolecraft.queue import MessageQueue

from .middleware import Middleware


class RetryableOptions(TypedDict, total=False):
    max_retries: int
    base_backoff_millis: int
    max_backoff_millis: int | None
    exponential_factor: int  # Exponential factor for backoff calculation
    jitter_range: int  # Random jitter range as a percentage of the base backoff time


class Retryable(Middleware):
    BASE_BACKOFF_MILLIS = 5 * 60 * 1000

    def __init__(
        self,
        queue: MessageQueue | None = None,
        **options: Unpack[RetryableOptions],
    ) -> None:
        # TODO: retry when
        # TODO: Retryable exceptions
        self.max_retries = options.get("max_retries", 3)
        self.base_backoff_millis = options.get(
            "base_backoff_millis", self.BASE_BACKOFF_MILLIS
        )
        self.max_backoff_millis = options.get("max_backoff_millis")
        self.exponential_factor = options.get("exponential_factor", 2)
        self.jitter_range = options.get("jitter_range", 0.2)
        self.options = options

        super().__init__(queue)

    def nack(self, message: Message, exception: Exception, **kwargs):
        assert self.queue, "Middleware Retryable is not initialized"
        retries = message.meta.retries
        if retries and retries >= self.max_retries:
            return self.queue.nack(message, exception=exception, **kwargs)
        delay_millis = int(self._compute_delay_millis(retries))
        return self.queue.retry(
            message, delay_millis=delay_millis, exception=exception
        )

    def _compute_delay_millis(self, retry_attempt: int):
        if retry_attempt == 0:
            return self.base_backoff_millis

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
