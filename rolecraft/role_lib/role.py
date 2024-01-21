import typing
from collections.abc import Callable
from typing import Unpack

from rolecraft.message import Message
from rolecraft.queue import EnqueueOptions, MessageQueue
from rolecraft.queue_factory import QueueConfigOptions, QueueFactory
from rolecraft.utils import typed_dict as _typed_dict

from . import error as _error
from .serializer import ParamsSerializerType, SerializedData


class RoleDefaultOptions(QueueConfigOptions, EnqueueOptions, total=False):
    queue_name: str


class DiaptchMessageOptions(RoleDefaultOptions, total=False):
    ...


class Role[**P, R, D: SerializedData]:
    """Role is a function wrapper that is extended with the functions related to
    the broker and message, such as send function data to the queue and
    """

    def __init__(
        self,
        fn: Callable[P, R],
        name: str | None = None,
        *,
        serializer: ParamsSerializerType[D],
        deserializer: ParamsSerializerType[SerializedData] | None = None,
        queue_factory: QueueFactory,
        **options: Unpack[RoleDefaultOptions],
    ) -> None:
        self.fn = fn
        self._name = name

        self.serializer = serializer
        self.deserializer = deserializer
        self.queue_factory = queue_factory

        self.options = options

    @property
    def name(self) -> str:
        return self._name or self.fn.__name__

    def __call__(self, *args: P.args, **kwds: P.kwargs) -> R:
        return self.fn(*args, **kwds)

    def craft(self, message: Message) -> R:
        if (
            "queue_name" in self.options
            and message.queue.name != self.options["queue_name"]
        ):
            raise _error.UnmatchedQueueNameError

        try:
            args, kwargs = self._deserialize(message.role_data)
        except Exception as e:
            raise _error.DeserializeError from e

        try:
            return self(*args, **kwargs)
        except Exception as e:
            raise _error.ActionError from e

    def _deserialize(self, data: SerializedData) -> tuple[tuple, dict]:
        if not data:
            return (), {}
        if self.deserializer:
            return self.deserializer.deserialize(self.fn, data)
        elif self.serializer.support(data):
            return self.serializer.deserialize(self.fn, data)
        else:
            raise RuntimeError("Unsupported data type")

    def dispatch_message(self, *args: P.args, **kwds: P.kwargs) -> Message:
        return self.dispatch_message_ext(args, kwds)

    @typing.overload
    def dispatch_message_ext(
        self,
        args: tuple = (),
        kwds: dict | None = None,
        *,
        raw_queue: MessageQueue | None = None,
        **options: Unpack[DiaptchMessageOptions],
    ) -> Message:
        ...

    @typing.overload
    def dispatch_message_ext(
        self,
        args: tuple = (),
        kwds: dict | None = None,
        *,
        raw_queue: MessageQueue | None = None,
        **options,
    ) -> Message:
        ...

    def dispatch_message_ext(
        self,
        args: tuple = (),
        kwds: dict | None = None,
        *,
        raw_queue: MessageQueue | None = None,
        **options,
    ) -> Message:
        updated_options = self.options.copy()
        updated_options.update(options)  # type: ignore

        if raw_queue:
            queue = self.queue_factory.build_queue(raw_queue=raw_queue)
        else:
            queue_configs = _typed_dict.subset_dict(
                updated_options, QueueConfigOptions
            )
            queue_name = options.pop("queue_name", "default")
            queue = self.queue_factory.build_queue(
                queue_name=queue_name, **queue_configs
            )

        message = self._build_message(queue, *args, **kwds or {})
        message.enqueue(**updated_options)
        return message

    def _build_message(
        self, queue: MessageQueue, *args: P.args, **kwds: P.kwargs
    ) -> Message:
        try:
            data = self.serializer.serialize(self.fn, args, kwds)
        except Exception as e:
            raise _error.SerializeError from e

        return Message(role_name=self.name, role_data=data, queue=queue)
