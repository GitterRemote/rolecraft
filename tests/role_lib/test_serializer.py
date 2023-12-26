import dataclasses


def test_serialize(str_serializer):
    def fn(a: int, b: str, *, c: float = 1.0):
        pass

    data = str_serializer.serialize(fn, args=(1, 2), kwds=dict(c=3.0))
    assert isinstance(data, str)

    args, kwds = str_serializer.deserialize(fn=fn, data=data)
    assert args == (1, 2)
    assert kwds == dict(c=3.0)
    assert isinstance(kwds["c"], float)


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
