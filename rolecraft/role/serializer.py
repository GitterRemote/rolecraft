import dataclasses
import inspect
import json
import warnings
from collections.abc import Callable
from typing import TypeGuard

SerializedData = str | bytes | None


class ParamsSerializer[D: SerializedData, A: tuple, K: dict]:
    def serialize(self, fn: Callable, args: A, kwds: K) -> D:
        raise NotImplementedError

    def deserialize(self, fn: Callable, data: D) -> tuple[A, K]:
        raise NotImplementedError

    def support(self, data) -> TypeGuard[D]:
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
        # TODO: implement _restore_kwds
        raise NotImplementedError

    def deserialize(self, fn: Callable, data: str) -> tuple[tuple, dict]:
        data_dict = json.loads(data)
        sig = inspect.signature(fn)
        args = self._restore_args(sig, data_dict.get("args", ()))
        kwds = self._restore_kwds(sig, data_dict.get("kwds", {}))
        return args, kwds

    def support(self, data) -> TypeGuard[str]:
        return isinstance(data, str)


class BytesParamsSerializer(ParamsSerializer[bytes, tuple, dict]):
    ...


class DefaultParamsSerializer(ParamsSerializer[SerializedData, tuple, dict]):
    def __init__(
        self,
        str_serializer: StrParamsSerializer,
        bytes_serializer: BytesParamsSerializer,
    ) -> None:
        self.str_serializer = str_serializer
        self.bytes_serializer = bytes_serializer

    def serialize(
        self, fn: Callable, args: tuple, kwds: dict
    ) -> SerializedData:
        return super().serialize(fn, args, kwds)

    def support(self, data) -> TypeGuard[SerializedData]:
        return (
            not data
            or self.str_serializer.support(data)
            or self.bytes_serializer.support(data)
        )


str_serializer = StrParamsSerializer()
bytes_serializer = BytesParamsSerializer()
default_serializer = DefaultParamsSerializer(str_serializer, bytes_serializer)
