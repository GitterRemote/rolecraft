import dataclasses
from typing import Any

import pytest


def test_serialize(str_serializer):
    def fn(a: int, b: str, *, c: float = 1.0):
        pass

    data = str_serializer.serialize(fn, args=(1, 2), kwds=dict(c=3.0))
    assert isinstance(data, str)

    args, kwds = str_serializer.deserialize(fn=fn, data=data)
    assert args == (1, 2)
    assert kwds == dict(c=3.0)
    assert isinstance(kwds["c"], float)


def test_serialize_empty_params(str_serializer):
    def fn():
        pass

    data = str_serializer.serialize(fn, (), {})
    assert isinstance(data, str)

    args, kwds = str_serializer.deserialize(fn=fn, data=data)
    assert args == ()
    assert kwds == {}


def test_serialize_with_dataclass_support(str_serializer):
    @dataclasses.dataclass
    class D:
        x: str
        y: int = 1

    def fn(a: int, b: str, *, c: float = 1.0, d: D):
        pass

    data = str_serializer.serialize(fn, args=(1, 2), kwds=dict(d=D("0")))
    assert isinstance(data, str)

    args, kwds = str_serializer.deserialize(fn=fn, data=data)
    assert args == (1, 2)
    assert kwds == dict(d=D("0"))


def test_serialize_with_callable(str_serializer):
    class CallableClass:
        def __call__(self, a: int, b: str, *, c: float = 1.0) -> Any:
            pass

    fn = CallableClass()

    data = str_serializer.serialize(fn, args=(1, 2), kwds=dict(c=3.0))
    assert isinstance(data, str)

    args, kwds = str_serializer.deserialize(fn=fn, data=data)
    assert args == (1, 2)
    assert kwds == dict(c=3.0)
    assert isinstance(kwds["c"], float)


def test_serialize_with_lambda(str_serializer):
    data = str_serializer.serialize(
        fn=lambda a, b, c: True, args=(1, 2), kwds=dict(c=3.0)
    )
    assert isinstance(data, str)

    args, kwds = str_serializer.deserialize(fn=lambda a, b, c: True, data=data)
    assert args == (1, 2)
    assert kwds == dict(c=3.0)
    assert isinstance(kwds["c"], float)


def test_serialize_with_kwargs(str_serializer):
    def fn(a: int, b: str, **kwds):
        pass

    data = str_serializer.serialize(fn, args=(1, 2), kwds=dict(c=3.0))
    assert isinstance(data, str)

    args, kwds = str_serializer.deserialize(fn=fn, data=data)
    assert args == (1, 2)
    assert kwds == dict(c=3.0)
    assert isinstance(kwds["c"], float)


def test_serialize_with_unmatched_functions(str_serializer):
    def fn_in(a: int, b: str, *, c: float = 1.0):
        pass

    def fn_out(a: int, b: str, *, d: list | None = None):
        pass

    data = str_serializer.serialize(fn_in, args=(1, 2), kwds=dict(c=3.0))
    assert isinstance(data, str)

    args, kwds = str_serializer.deserialize(fn=fn_out, data=data)
    assert args == (1, 2)
    assert kwds == {}

    def fn_out2(a: int, b: str, **kwds):
        pass

    args, kwds = str_serializer.deserialize(fn=fn_out2, data=data)
    assert args == (1, 2)
    assert kwds == dict(c=3.0)
    assert isinstance(kwds["c"], float)


class TestHybridDeserializer:
    @pytest.fixture()
    def deserializer(self, hybrid_deserializer):
        return hybrid_deserializer

    def test_deserialize_str(self, deserializer, str_serializer):
        def fn(a: int, b: str, *, c: float = 1.0):
            pass

        data = str_serializer.serialize(fn, args=(1, 2), kwds=dict(c=3.0))
        assert isinstance(data, str)

        args, kwds = deserializer.deserialize(fn=fn, data=data)
        assert args == (1, 2)
        assert kwds == dict(c=3.0)
        assert isinstance(kwds["c"], float)

    def test_serialize_empty_params(self, deserializer, str_serializer):
        def fn():
            pass

        data = str_serializer.serialize(fn, (), {})
        assert isinstance(data, str)

        args, kwds = str_serializer.deserialize(fn=fn, data=data)
        assert args == ()
        assert kwds == {}
