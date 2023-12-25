import pytest
from unittest import mock
from rolecraft.config import queue_config as queue_config_mod


@pytest.fixture()
def broker():
    return mock.MagicMock()


@pytest.fixture()
def encoder():
    return mock.MagicMock()


@pytest.fixture()
def queue_config(broker, encoder):
    return queue_config_mod.QueueConfig(broker=broker, encoder=encoder)


@pytest.fixture()
def incomplete_queue_config(encoder):
    return queue_config_mod.IncompleteQueueConfig(encoder=encoder)
