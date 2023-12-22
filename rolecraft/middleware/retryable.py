import random
from rolecraft.message import Message
from .middleware import Middleware


class Retryable(Middleware):
    BASE_BACKOFF_MILLIS = 5 * 60 * 1000

    def __init__(
        self,
        *,
        max_retries=3,
        base_backoff_millis=BASE_BACKOFF_MILLIS,
        max_backoff_millis: int | None = None,
        exponential_factor=2,  # Exponential factor for backoff calculation
        jitter_range=0.2,  # Random jitter range as a percentage of the base backoff time
    ) -> None:
        # TODO: retry when
        # TODO: Retryable exceptions
        self.max_retries = max_retries
        self.base_backoff_millis = base_backoff_millis
        self.max_backoff_millis = max_backoff_millis
        self.exponential_factor = exponential_factor
        self.jitter_range = jitter_range

        super().__init__()

    def nack(self, message: Message, exception: Exception, **kwargs):
        retries = message.meta.retries
        if retries and retries >= self.max_retries:
            return self.queue.nack(message, exception=exception, **kwargs)
        # TODO: options from roles?
        delay_millis = self._compute_delay(retries)
        return self.queue.retry(
            message, delay_millis=delay_millis, exception=exception
        )

    def _compute_delay(self, retry_attempt: int):
        if retry_attempt == 0:
            return self.base_backoff_millis

        if retry_attempt == 0:
            return self.base_backoff_millis

        backoff_time = self.base_backoff_millis * (
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
