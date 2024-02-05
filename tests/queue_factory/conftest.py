from unittest import mock

import pytest

from rolecraft.queue import queue_config as queue_config_mod
from rolecraft.queue_factory import queue_factory as queue_factory_mod
from rolecraft.queue_factory import (
    cached_queue_factory as cached_queue_factory_mod,
)


@pytest.fixture()
def broker():
    return mock.MagicMock()


@pytest.fixture()
def encoder():
    return mock.MagicMock()


@pytest.fixture()
def broker2():
    return mock.MagicMock()


@pytest.fixture()
def encoder2():
    return mock.MagicMock()


@pytest.fixture()
def queue_config(broker, encoder):
    return queue_config_mod.QueueConfig(broker=broker, encoder=encoder)


@pytest.fixture()
def config_fetcher(queue_config):
    def fetch(queue_name=None, **kwargs):
        return queue_config.replace(**kwargs)

    return fetch


@pytest.fixture()
def queue_factory(config_fetcher):
    return queue_factory_mod.QueueFactory(config_fetcher=config_fetcher)


@pytest.fixture()
def cached_queue_factory(config_fetcher):
    return cached_queue_factory_mod.CachedQueueFactory(
        config_fetcher=config_fetcher
    )
