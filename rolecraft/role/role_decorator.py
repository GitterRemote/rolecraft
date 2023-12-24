from collections.abc import Callable

from . import role_hanger as _role_hanger
from . import serializer as _serializer
from .role import Role
from .role_hanger import RoleHanger
from .serializer import ParamsSerializerType, SerializedData


class RoleDecorator[**P, R, D: SerializedData]:
    def __init__(
        self,
        *,
        queue_name: str | None = None,
        serializer: ParamsSerializerType[D] | None = None,
        deserializer: ParamsSerializerType[SerializedData] | None = None,
        role_hanger: RoleHanger | None = None,
        **options,
    ) -> None:
        self.queue_name = queue_name if queue_name is not None else "default"
        self.serializer = serializer
        self.deserializer = deserializer or _serializer.default_serializer
        self.role_hanger = role_hanger or _role_hanger.default_role_hanger
        self.options = options

    def __call__(
        self,
        fn: Callable[P, R],
        name: str | None = None,
        *,
        queue_name: str | None,
        serializer: ParamsSerializerType[D] | None,
        deserializer: ParamsSerializerType[SerializedData] | None = None,
        role_hanger: RoleHanger | None = None,
        **options,
    ) -> Role[P, R, D] | Role[P, R, str]:
        queue_name = queue_name if queue_name is not None else self.queue_name
        serializer = serializer or self.serializer
        default_options = self.options.copy()
        default_options.update(options)

        if serializer is None:
            return Role(
                fn=fn,
                name=name,
                queue_name=queue_name,
                serializer=_serializer.str_serializer,
                deserializer=deserializer or self.deserializer,
                role_hanger=role_hanger or self.role_hanger,
                **default_options,
            )
        else:
            return Role(
                fn=fn,
                name=name,
                queue_name=queue_name,
                serializer=serializer,
                deserializer=deserializer or self.deserializer,
                role_hanger=role_hanger or self.role_hanger,
                **options,
            )
