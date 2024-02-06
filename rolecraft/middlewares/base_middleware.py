import dataclasses
from typing import Any, Self

from rolecraft.queue import Message, MessageQueue, Middleware, MiddlewareError


class UninitiatedError(MiddlewareError):
    """Middleware is not initialzied with a Queue"""

    ...


class BaseMiddleware(Middleware):
    @dataclasses.dataclass
    class Meta:
        @classmethod
        def create_from(cls, meta_data: dict[str, str | int | float]) -> Self:
            return cls(
                **{
                    field.name: meta_data.get(field.name)
                    for field in dataclasses.fields(cls)
                    if field.name in meta_data
                }
            )

    @property
    def _guarded_queue(self) -> MessageQueue:
        if not self.queue:
            raise UninitiatedError
        return self.queue

    @property
    def options(self) -> dict[str, Any]:
        return {}

    def __call__(self, queue: MessageQueue) -> MessageQueue:
        if isinstance(queue, Outermost):
            outermost, queue = queue, queue._guarded_queue
        else:
            outermost = Outermost()

        wrapped = self.copy_with(queue)
        assert isinstance(wrapped, MessageQueue)

        if not isinstance(wrapped, Outermost):
            return outermost(wrapped)
        return wrapped

    def copy_with(self, queue: MessageQueue) -> Self:
        return self.__class__(queue=queue, **self.options)

    def __repr__(self) -> str:
        if self.queue:
            return f"{self.__class__.__name__}({repr(self.queue)})"
        return f"{self.__class__.__name__}(options={repr(self.options)})"


class Outermost(BaseMiddleware):
    """Outermost Middleware to update the `Message.queue` attribute to `self`."""

    def _update_messages(self, messages: list[Message]) -> list[Message]:
        for message in messages:
            message.queue = self  # type: ignore
        return messages

    def block_receive(self, *args, **kwargs):
        """If the wait_time_seconds is None, it will be default value of the
        queue."""
        future = self._guarded_queue.block_receive(*args, **kwargs)
        return future.transform(self._update_messages)

    def receive(self, *args, **kwargs):
        msgs = self._guarded_queue.receive(*args, **kwargs)
        return self._update_messages(msgs)
