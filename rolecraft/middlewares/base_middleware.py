from typing import Any, Self

from rolecraft.message import Message
from rolecraft.middleware import Middleware, MiddlewareError
from rolecraft.queue import MessageQueue


class UninitiatedError(MiddlewareError):
    """Middleware is not initialzied with a Queue"""

    ...


class BaseMiddleware(Middleware):
    def __init__(self, queue: MessageQueue | None = None) -> None:
        super().__init__(queue=queue)
        self.is_outmost = False

    @property
    def _guarded_queue(self) -> MessageQueue:
        if not self.queue:
            raise UninitiatedError
        return self.queue

    @property
    def options(self) -> dict[str, Any]:
        return {}

    def __call__(self, queue: MessageQueue) -> Self:
        wrapped = self.copy_with(queue)

        # Update is_outmost attribute
        if getattr(queue, "is_outmost", False) is True:
            queue.is_outmost = False  # type: ignore
        wrapped.is_outmost = True

        return wrapped

    def copy_with(self, queue: MessageQueue) -> Self:
        return self.__class__(queue=queue, **self.options)

    # -- Update message's queue attribute --

    def _update_message(self, message: Message) -> Message:
        message.queue = self  # type: ignore
        return message

    def block_receive(self, *args, **kwargs):
        """If the wait_time_seconds is None, it will be default value of the
        queue."""
        future = self._guarded_queue.block_receive(*args, **kwargs)
        return (
            future.transform(
                lambda msgs: list(map(self._update_message, msgs))
            )
            if self.is_outmost
            else future
        )

    def receive(self, *args, **kwargs):
        msgs = self._guarded_queue.receive(*args, **kwargs)
        if self.is_outmost:
            return [self._update_message(m) for m in msgs]
        return msgs
