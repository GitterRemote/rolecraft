from collections.abc import Callable

from . import broker as _broker
from .broker import Broker
from .message import Message
from .role_hanger import RoleHanger
from .queue import MessageQueue


class ParamsSerializer[D: str | bytes, A, K]:
    def serialize(self, args: A, kwds: K) -> D:
        raise NotImplementedError

    def deserialize(self, data: D) -> tuple[A, K]:
        raise NotImplementedError


class Role[**P, R, D]:
    def __init__(
        self,
        fn: Callable[P, R],
        name: str | None = None,
        *,
        queue_name: str,
        serializer: ParamsSerializer,
        role_hanger: RoleHanger,
        **options,
    ) -> None:
        self.fn = fn
        self._name = name
        self.queue_name = queue_name

        self.serializer = serializer
        self.role_hanger = role_hanger

        self.options = options

    @property
    def name(self) -> str:
        return self._name or self.fn.__name__

    def __call__(self, *args: P.args, **kwds: P.kwargs) -> R:
        return self.fn(*args, **kwds)

    def craft(self, data) -> R:
        args, kwargs = self.serializer.deserialize(data)
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
        data = self.serializer.serialize(args, kwds)
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
