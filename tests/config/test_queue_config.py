import dataclasses
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
def incomplete_queue_config(broker, encoder):
    return queue_config_mod.IncompleteQueueConfig(encoder=encoder)


def test_to_queue_config(incomplete_queue_config, broker, queue_config):
    with pytest.raises(Exception):
        incomplete_queue_config.to_queue_config()
    config = dataclasses.replace(incomplete_queue_config, broker=broker)
    assert config.to_queue_config() == queue_config
