import dataclasses
import inspect
import json
import types
import typing
from collections.abc import Callable
from typing import Any, TypeGuard

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
        return json.dumps(dict(a=args_data, k=kwds_data))

    def _convert(self, value):
        if dataclasses.is_dataclass(value):
            value = dataclasses.asdict(value)
        return value

    def _unwrap_optional(self, annotation: Any) -> Any:
        origin = typing.get_origin(annotation)
        if origin is types.UnionType:
            args = typing.get_args(annotation)
            if len(args) == 2 and type(None) in args:
                if args[1] is type(None):
                    return args[0]
                return args[1]

        return annotation

    def _restore(self, param: inspect.Parameter, value):
        annotation = self._unwrap_optional(param.annotation)
        if dataclasses.is_dataclass(annotation):
            return annotation(**value)
        return value

    def _restore_args(
        self, sig: inspect.Signature, args: tuple | list
    ) -> tuple:
        return tuple(
            self._restore(param, value)
            for value, param in zip(args, sig.parameters.values())
        )

    def _restore_kwds(self, sig: inspect.Signature, kwds: dict) -> dict:
        params = list(sig.parameters.values())
        has_var_kwds = (
            params and params[-1].kind == inspect.Parameter.VAR_KEYWORD
        )
        if has_var_kwds:
            return {
                k: self._restore(sig.parameters[k], v)
                if k in sig.parameters
                else v
                for k, v in kwds.items()
            }
        else:
            return {
                k: self._restore(sig.parameters[k], v)
                for k, v in kwds.items()
                if k in sig.parameters
            }

    def deserialize(self, fn: Callable, data: str) -> tuple[tuple, dict]:
        data_dict = json.loads(data)
        sig = inspect.signature(fn, eval_str=True)
        args = self._restore_args(sig, data_dict.get("a", ()))
        kwds = self._restore_kwds(sig, data_dict.get("k", {}))
        return args, kwds

    def support(self, data) -> TypeGuard[str]:
        return isinstance(data, str)


class BytesParamsSerializer(ParamsSerializer[bytes, tuple, dict]):
    ...


class HybridParamsDeserializer(ParamsSerializer[SerializedData, tuple, dict]):
    def __init__(
        self,
        str_serializer: StrParamsSerializer,
        bytes_serializer: BytesParamsSerializer,
    ) -> None:
        self.str_serializer = str_serializer
        self.bytes_serializer = bytes_serializer

    def deserialize(
        self, fn: Callable, data: SerializedData
    ) -> tuple[tuple, dict]:
        if not data:
            return (), {}
        elif self.str_serializer.support(data):
            return self.str_serializer.deserialize(fn, data)
        else:
            raise NotImplementedError

    def support(self, data) -> TypeGuard[SerializedData]:
        return (
            not data
            or self.str_serializer.support(data)
            or self.bytes_serializer.support(data)
        )


str_serializer = StrParamsSerializer()
bytes_serializer = BytesParamsSerializer()
hybrid_deserializer = HybridParamsDeserializer(
    str_serializer, bytes_serializer
)
