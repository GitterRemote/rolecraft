from unittest import mock

import pytest

from rolecraft import message as message_mod
from rolecraft.role_lib import role as role_mod


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


def test_dispatch_message(role):
    with mock.patch.object(role, "dispatch_message_ext") as mocked_method:
        role.dispatch_message(1, "b", c=["c"])
        mocked_method.assert_called_once_with((1, "b"), dict(c=["c"]))


def test_dispatch_message_ext(role, queue, queue_factory):
    args = (1, "b")
    kwds = dict(c=["c"])
    queue.enqueue.return_value = True
    assert not queue.enqueue.side_effect

    msg = role.dispatch_message_ext(args, kwds)
    assert msg
    assert msg.queue is queue
    queue.enqueue.assert_called_once_with(msg)
    queue_factory.get_or_build.assert_called_once_with(queue_name="default")


def test_dispatch_message_ext_with_options(role, queue):
    args = (1, "b")
    kwds = dict(c=["c"])
    queue.enqueue.return_value = True

    options = {"delay_millis": 1999}
    msg = role.dispatch_message_ext(args, kwds, **options)
    queue.enqueue.assert_called_once_with(msg, **options)


def test_dispatch_message_ext_failed(role, queue):
    args = (1, "b")
    kwds = dict(c=["c"])
    queue.enqueue.return_value = False

    with pytest.raises(RuntimeError):
        role.dispatch_message_ext(args, kwds)
