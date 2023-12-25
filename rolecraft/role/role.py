from collections.abc import Callable

from rolecraft.broker import Broker
from rolecraft.message import Message
from rolecraft.queue import MessageQueue
from rolecraft.queue_manager import QueueManager

from .role_hanger import RoleHanger
from .serializer import ParamsSerializerType, SerializedData


class Role[**P, R, D: SerializedData]:
    """Role is a function wrapper that is extended with the functions related to
    the broker and message, such as send function data to the queue and
    """

    def __init__(
        self,
        fn: Callable[P, R],
        name: str | None = None,
        *,
        queue_name: str,
        serializer: ParamsSerializerType[D],
        deserializer: ParamsSerializerType[SerializedData] | None = None,
        role_hanger: RoleHanger,
        queue_manager: QueueManager,
        **options,
    ) -> None:
        self.fn = fn
        self._name = name
        self.queue_name = queue_name

        self.serializer = serializer
        self.deserializer = deserializer
        self.role_hanger = role_hanger
        self.queue_manager = queue_manager

        self.options = options

    @property
    def name(self) -> str:
        return self._name or self.fn.__name__

    def __call__(self, *args: P.args, **kwds: P.kwargs) -> R:
        return self.fn(*args, **kwds)

    def craft(self, message: Message) -> R:
        return self._craft(message.role_data)

    def _craft(self, data: SerializedData | D) -> R:
        if not data:
            return self()

        if self.deserializer:
            args, kwargs = self.deserializer.deserialize(self.fn, data)
        elif self.serializer.support(data):
            args, kwargs = self.serializer.deserialize(self.fn, data)
        else:
            raise RuntimeError("Unsupported data type")
        return self(*args, **kwargs)

    def dispatch_message(self, *args: P.args, **kwds: P.kwargs) -> Message:
        return self.dispatch_message_ext(args, kwds)

    def dispatch_message_ext(
        self,
        args,
        kwds,
        *,
        queue_name: str | None = None,
        broker: Broker | None = None,
        raw_queue: MessageQueue | None = None,
        **options,
    ) -> Message:
        if raw_queue:
            queue = self.queue_manager.get_or_bulid(raw_queue=raw_queue)
        else:
            # TODO: add queue config keys and encoder
            queue = self.queue_manager.get_or_bulid(
                queue_name=queue_name or self.queue_name or "default",
                broker=broker,
            )

        message = self._build_message(queue, *args, **kwds)
        # TODO: add role-specific enqueue parameters, replace options with typeddict
        if not message.enqueue(**options):
            raise RuntimeError(
                f"Dispatch message error: enqueue error for {message}"
            )
        return message

    def _build_message(
        self, queue: MessageQueue, *args: P.args, **kwds: P.kwargs
    ) -> Message:
        data = self.serializer.serialize(self.fn, args, kwds)
        return Message(role_name=self.name, role_data=data, queue=queue)
