from unittest import mock

import pytest

from rolecraft.role_lib import role_decorator as role_decorator_mod
from rolecraft.role_lib import role_hanger as role_hanger_mod


@pytest.fixture()
def role_hanger():
    return role_hanger_mod.SimpleRoleHanger()


@pytest.fixture()
def role_decorator(role_hanger, queue_factory):
    return role_decorator_mod.RoleDecorator(
        role_hanger=role_hanger,
        queue_factory=queue_factory,
    )


def test_decorate(role_decorator, queue_factory):
    fn_inner = mock.Mock()

    @role_decorator
    def fn(a: int, b: str = "2", *, c: list | None = None) -> bool:
        fn_inner(a, b, c=c)
        return True

    assert fn.name == "fn"
    assert fn.queue_factory is queue_factory

    fn(1)
    fn_inner.assert_called_once_with(1, "2", c=None)


def test_decorate_with_parenthesis(role_decorator):
    fn_inner = mock.Mock()

    @role_decorator()
    def fn(a: int, b: str = "2", *, c: list | None = None) -> bool:
        fn_inner(a, b, c=c)
        return True

    assert fn.name == "fn"

    fn(1)
    fn_inner.assert_called_once_with(1, "2", c=None)


def test_decorate_with_options(role_decorator, str_serializer):
    fn_inner = mock.Mock()
    broker = mock.Mock()

    @role_decorator(
        "role_fn",
        deserializer=str_serializer,
        wait_time_seconds=100,
        broker=broker,
    )
    def fn(a: int, b: str = "2", *, c: list | None = None) -> bool:
        fn_inner(a, b, c=c)
        return True

    assert fn.name == "role_fn"
    assert fn.options.get("queue_name") is None
    assert fn.options.get("broker") is broker
    assert fn.deserializer is str_serializer

    fn(1)
    fn_inner.assert_called_once_with(1, "2", c=None)
