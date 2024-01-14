from unittest import mock

import pytest

from rolecraft.role_lib import serializer as serializer_mod


@pytest.fixture
def str_serializer():
    return serializer_mod.str_serializer


@pytest.fixture
def hybrid_deserializer():
    return serializer_mod.hybrid_deserializer


@pytest.fixture()
def queue():
    return mock.MagicMock()


@pytest.fixture()
def queue_factory(queue):
    factory = mock.MagicMock()
    factory.build_queue.return_value = queue
    return factory
