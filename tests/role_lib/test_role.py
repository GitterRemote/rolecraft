import pytest
from unittest import mock
from rolecraft.role_lib import role_decorator as role_decorator_mod
from rolecraft.role_lib import role as role_mod


@pytest.fixture
def queue_factory():
    return mock.MagicMock()


@pytest.fixture
def fn():
    return mock.MagicMock()


@pytest.fixture
def role(fn, str_serializer, queue_factory):
    return role_mod.Role(
        fn, serializer=str_serializer, queue_factory=queue_factory
    )
