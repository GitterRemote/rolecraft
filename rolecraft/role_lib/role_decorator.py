from collections.abc import Callable
from typing import Unpack

from rolecraft import role_hanger as _role_hanger
from rolecraft.config import ConfigStore
from rolecraft.queue_factory import QueueFactory
from rolecraft.role_hanger import RoleHanger

from . import serializer as _serializer
from .role import Role, RoleDefaultOptions
from .serializer import ParamsSerializerType, SerializedData


class RoleDecorator[**P, R, D: SerializedData]:
    def __init__(
        self,
        *,
        serializer: ParamsSerializerType[D] | None = None,
        deserializer: ParamsSerializerType[SerializedData] | None = None,
        queue_factory: QueueFactory | None = None,
        role_hanger: RoleHanger | None = None,
        config_store: ConfigStore | None = None,
        queue_name: str | None = None,
        **options: Unpack[RoleDefaultOptions],
    ) -> None:
        self.serializer = serializer
        self.deserializer = deserializer or _serializer.default_serializer
        self.queue_factory = queue_factory or QueueFactory(
            config_fetcher=config_store.fetcher if config_store else None
        )
        self.role_hanger = role_hanger or _role_hanger.default_role_hanger

        self.queue_name = queue_name
        self.options = options

    def __call__(
        self,
        fn: Callable[P, R],
        name: str | None = None,
        *,
        serializer: ParamsSerializerType[D] | None = None,
        deserializer: ParamsSerializerType[SerializedData] | None = None,
        queue_factory: QueueFactory | None = None,
        role_hanger: RoleHanger | None = None,
        config_store: ConfigStore | None = None,
        queue_name: str | None = None,
        **options: Unpack[RoleDefaultOptions],
    ) -> Role[P, R, D] | Role[P, R, str]:
        serializer = serializer or self.serializer
        queue_factory = queue_factory or (
            QueueFactory(config_fetcher=config_store.fetcher)
            if config_store
            else self.queue_factory
        )

        queue_name = queue_name if queue_name is not None else self.queue_name
        default_options = self.options.copy()
        default_options.update(options)
        options = default_options

        if serializer is None:
            return Role(
                fn=fn,
                name=name,
                serializer=_serializer.str_serializer,
                deserializer=deserializer or self.deserializer,
                role_hanger=role_hanger or self.role_hanger,
                queue_factory=queue_factory,
                queue_name=queue_name,
                **options,
            )
        else:
            return Role(
                fn=fn,
                name=name,
                serializer=serializer,
                deserializer=deserializer or self.deserializer,
                role_hanger=role_hanger or self.role_hanger,
                queue_factory=queue_factory,
                queue_name=queue_name,
                **options,
            )
