from collections.abc import Callable
import typing
from typing import Any
import json
import dataclasses
import inspect
import warnings

from . import broker as _broker
from .broker import Broker
from .message import Message
from .role_hanger import RoleHanger
from .queue import MessageQueue

SerializedData = str | bytes


class ParamsSerializer[D: SerializedData, A: tuple, K: dict]:
    def serialize(self, fn: Callable, args: A, kwds: K) -> D:
        raise NotImplementedError

    def deserialize(self, fn: Callable, data: D) -> tuple[A, K]:
        raise NotImplementedError


type ParamsSerializerType[D: SerializedData] = ParamsSerializer[D, tuple, dict]


class StrParamsSerializer(ParamsSerializer[str, tuple, dict]):
    def serialize(self, fn: Callable, args: tuple, kwds: dict) -> str:
        args_data = [self._convert(v) for v in args]
        kwds_data = {k: self._convert(v) for k, v in kwds.items()}
        return json.dumps(dict(args=args_data, kwds=kwds_data))

    def _convert(self, value):
        if dataclasses.is_dataclass(value):
            value = dataclasses.asdict(value)
        return value

    def _restore(self, param: inspect.Parameter, value):
        if isinstance(param.annotation, str):
            warnings.warn("seems annotation doesn't work in runtime")
            return value
        if dataclasses.is_dataclass(param.annotation):
            return param.annotation(**value)
        else:
            return value

    def _restore_args(
        self, sig: inspect.Signature, args: tuple | list
    ) -> tuple:
        return tuple(
            self._restore(param, value)
            for value, param in zip(args, sig.parameters.values())
        )

    def _restore_kwds(self, sig: inspect.Signature, kwds: dict) -> dict:
        raise NotImplementedError

    def deserialize(self, fn: Callable, data: str) -> tuple[tuple, dict]:
        data_dict = json.loads(data)
        sig = inspect.signature(fn)
        args = self._restore_args(sig, data_dict.get("args", ()))
        kwds = self._restore_kwds(sig, data_dict.get("kwds", {}))
        return args, kwds


class Role[**P, R, D: SerializedData]:
    def __init__(
        self,
        fn: Callable[P, R],
        name: str | None = None,
        *,
        queue_name: str,
        serializer: ParamsSerializerType[D],
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

    def craft(self, data: D) -> R:
        args, kwargs = self.serializer.deserialize(self.fn, data)
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


def role[**P, R, D: SerializedData](
    fn: Callable[P, R],
    name: str | None = None,
    *,
    queue_name: str = "default",
    serializer: ParamsSerializerType[D] | None,
    role_hanger=RoleHanger(),
    **options,
) -> Role[P, R, D] | Role[P, R, str]:
    if serializer is not None:
        return Role(
            fn,
            name=name,
            queue_name=queue_name,
            serializer=serializer,
            role_hanger=role_hanger,
            **options,
        )
    else:
        return Role(
            fn,
            name=name,
            queue_name=queue_name,
            serializer=StrParamsSerializer(),
            role_hanger=role_hanger,
            **options,
        )
