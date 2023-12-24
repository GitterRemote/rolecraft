from collections.abc import Callable

from rolecraft import broker as _broker
from rolecraft.broker import Broker
from rolecraft.message import Message
from rolecraft.queue import MessageQueue

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
        **options,
    ) -> None:
        self.fn = fn
        self._name = name
        self.queue_name = queue_name

        self.serializer = serializer
        self.deserializer = deserializer
        self.role_hanger = role_hanger

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
        queue: MessageQueue | None = None,
        broker: Broker | None = None,
        **options,
    ) -> Message:
        queue = self._get_queue(
            queue_name=queue_name, queue=queue, broker=broker
        )
        message = self._build_message(queue, *args, **kwds)
        # TODO: add role-specific enqueue parameters
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

    def _get_queue(
        self,
        queue_name: str | None = None,
        queue: MessageQueue | None = None,
        broker: Broker | None = None,
    ) -> MessageQueue:
        # TODO: how to configure & get middlewares for dispatching?
        raise NotImplementedError

    def _get_broker(self) -> Broker:
        # TODO: discussion: could the role have a bounded broker?
        broker = _broker.default_broker
        if not broker:
            raise RuntimeError("Broker is required to be set or bound")
        return broker
