import functools
import typing
from collections.abc import Callable
from typing import Unpack

from rolecraft.queue_factory import (
    CachedQueueFactory,
    ConfigFetcher,
    QueueFactory,
)

from . import role_hanger as _role_hanger
from . import serializer as _serializer
from .role import Role, RoleDefaultOptions
from .role_hanger import RoleHanger
from .serializer import ParamsSerializerType, SerializedData


class RoleDecorator[D: SerializedData]:
    def __init__(
        self,
        *,
        serializer: ParamsSerializerType[D] | None = None,
        deserializer: ParamsSerializerType[SerializedData] | None = None,
        queue_factory: QueueFactory | None = None,
        config_fetcher: ConfigFetcher | None = None,
        role_hanger: RoleHanger | None = None,
        **options: Unpack[RoleDefaultOptions],
    ) -> None:
        self.serializer = serializer
        self.deserializer = deserializer or _serializer.hybrid_deserializer

        if not queue_factory:
            if not config_fetcher:
                raise ValueError(
                    "ConfigFetcher is required if no QueueFactory is provided"
                )
            queue_factory = CachedQueueFactory(config_fetcher=config_fetcher)

        self.queue_factory = queue_factory
        self.role_hanger = (
            role_hanger
            if role_hanger is not None
            else _role_hanger.default_role_hanger
        )

        self.options = options

    @typing.overload
    def __call__[**P, R](self, fn: Callable[P, R], /) -> Role[P, R, str]:
        ...

    @typing.overload
    def __call__[**P, R](
        self,
        name: str | None = None,
        *,
        deserializer: ParamsSerializerType[SerializedData] | None = None,
        **options: Unpack[RoleDefaultOptions],
    ) -> Callable[[Callable[P, R]], Role[P, R, str]]:
        ...

    @typing.overload
    def __call__[**P, R](
        self,
        name: str | None = None,
        *,
        serializer: ParamsSerializerType[D],
        deserializer: ParamsSerializerType[SerializedData] | None = None,
        **options: Unpack[RoleDefaultOptions],
    ) -> Callable[[Callable[P, R]], Role[P, R, D]]:
        ...

    def __call__(self, name=None, **kwds):  # type: ignore
        if callable(name):
            return self._wrap(fn=name)

        return functools.partial(self._wrap, name=name, **kwds)

    def _wrap[**P, R](
        self,
        fn: Callable[P, R],
        name: str | None = None,
        *,
        serializer: ParamsSerializerType[D] | None = None,
        deserializer: ParamsSerializerType[SerializedData] | None = None,
        **options: Unpack[RoleDefaultOptions],
    ) -> Role[P, R, D] | Role[P, R, str]:
        serializer = serializer or self.serializer

        default_options = self.options.copy()
        default_options.update(options)
        options = default_options

        if serializer is None:
            role = Role(
                fn=fn,
                name=name,
                serializer=_serializer.str_serializer,
                deserializer=deserializer or self.deserializer,
                queue_factory=self.queue_factory,
                **options,
            )
        else:
            role = Role(
                fn=fn,
                name=name,
                serializer=serializer,
                deserializer=deserializer or self.deserializer,
                queue_factory=self.queue_factory,
                **options,
            )

        self.role_hanger.put(role)
        return role
