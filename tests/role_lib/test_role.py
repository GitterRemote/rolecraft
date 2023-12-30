import pytest
from unittest import mock
from rolecraft.role_lib import role_decorator as role_decorator_mod
from rolecraft.role_lib import role as role_mod
from rolecraft import message as message_mod


@pytest.fixture()
def queue():
    return mock.MagicMock()


@pytest.fixture()
def queue_factory(queue):
    factory = mock.MagicMock()
    factory.get_or_build.side_effect = [queue]
    return factory


@pytest.fixture()
def fn():
    mocked = mock.MagicMock()
    mocked.__name__ = "mocked_fn_name"
    return mocked


@pytest.fixture()
def serializer(str_serializer):
    return str_serializer


@pytest.fixture()
def role(fn, serializer, queue_factory):
    return role_mod.Role(
        fn, serializer=serializer, queue_factory=queue_factory
    )


def test_name(fn, role):
    assert role.name == fn.__name__

    role._name = "another_name"
    assert role.name == "another_name"


def test_call(role, fn):
    rv = mock.MagicMock()
    fn.return_value = rv
    assert role(1, "b", c=["c"]) is rv
    fn.assert_called_once_with(1, "b", c=["c"])

    fn.reset_mock()

    role()
    fn.assert_called_once_with()


def test_craft(role, serializer, fn, queue):
    rv = mock.MagicMock()
    fn.return_value = rv

    data = serializer.serialize(fn=fn, args=(1, "b"), kwds=dict(c=["c"]))
    msg = message_mod.Message(role_data=data, role_name=role.name, queue=queue)
    assert role.craft(msg) is rv
    fn.assert_called_once_with(1, "b", c=["c"])
