import pytest
from unittest import mock
from rolecraft.config import queue_config as queue_config_mod
from rolecraft.config import config_store as config_store_mod

DefaultConfigStore = config_store_mod.DefaultConfigStore


@pytest.fixture()
def broker():
    return mock.MagicMock()


@pytest.fixture()
def broker2():
    return mock.MagicMock()


@pytest.fixture()
def encoder():
    return mock.MagicMock()

@pytest.fixture()
def encoder2():
    return mock.MagicMock()


@pytest.fixture()
def queue_config(broker, encoder):
    return queue_config_mod.QueueConfig(broker=broker, encoder=encoder)


@pytest.fixture()
def queue_config2(broker, encoder, middlewares):
    return queue_config_mod.QueueConfig(
        broker=broker, encoder=encoder, middlewares=middlewares
    )


@pytest.fixture()
def queue_config3(broker2, encoder, middlewares):
    return queue_config_mod.QueueConfig(
        broker=broker2, encoder=encoder, middlewares=middlewares
    )


@pytest.fixture()
def queue_config4(broker2, encoder):
    return queue_config_mod.QueueConfig(broker=broker2, encoder=encoder)


@pytest.fixture()
def incomplete_queue_config(encoder):
    return queue_config_mod.IncompleteQueueConfig(encoder=encoder)


@pytest.fixture()
def middlewares():
    return [mock.MagicMock(), mock.MagicMock()]


@pytest.fixture()
def default_only_store(queue_config):
    return DefaultConfigStore(queue_config=queue_config)


@pytest.fixture()
def queue_configs(queue_config2, queue_config3):
    return {
        "queue2": queue_config2,
        "queue3": queue_config3,
    }


@pytest.fixture()
def queue_configs_only_store(queue_config, queue_configs):
    return DefaultConfigStore(
        queue_config=queue_config, queue_configs=queue_configs
    )


@pytest.fixture()
def broker_queue_configs(queue_config4, broker2):
    assert queue_config4.broker is broker2
    return {
        queue_config4.broker: queue_config4,
    }


@pytest.fixture()
def broker_queue_configs_only_store(queue_config, broker_queue_configs):
    return DefaultConfigStore(
        queue_config=queue_config, broker_queue_configs=broker_queue_configs
    )


@pytest.fixture()
def hybrid_queue_configs__store(
    queue_config, queue_configs, broker_queue_configs
):
    return DefaultConfigStore(
        queue_config=queue_config,
        queue_configs=queue_configs,
        broker_queue_configs=broker_queue_configs,
    )
