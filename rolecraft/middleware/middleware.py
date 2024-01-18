from typing import Self

from rolecraft.message import Message
from rolecraft.queue import MessageQueue


class UninitiatedError(Exception):
    ...


@MessageQueue.register
class Middleware:
    def __init__(self, queue: MessageQueue | None = None) -> None:
        self.queue: MessageQueue | None = queue
        self.is_outmost = False

    def __getattr__(self, name):
        return getattr(self.queue, name)

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
        if not self.queue:
            raise UninitiatedError

        future = self.queue.block_receive(*args, **kwargs)
        return (
            future.transform(
                lambda msgs: list(map(self._update_message, msgs))
            )
            if self.is_outmost
            else future
        )

    def receive(self, *args, **kwargs):
        if not self.queue:
            raise UninitiatedError

        msgs = self.queue.receive(*args, **kwargs)
        if self.is_outmost:
            return [self._update_message(m) for m in msgs]
        return msgs
