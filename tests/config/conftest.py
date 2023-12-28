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
def broker_queue_config_only_store(queue_config, queue_config3):
    assert queue_config3.broker is not queue_config.broker
    broker_queue_config = {
        queue_config.broker: queue_config,
        queue_config3.broker: queue_config3,
    }
    return DefaultConfigStore(
        queue_config=queue_config, broker_queue_config=broker_queue_config
    )


@pytest.fixture()
def broker_queue_configs_only_store(
    queue_config, queue_config2, queue_config3
):
    assert queue_config.broker is queue_config2.broker
    broker_queue_configs = {
        queue_config2.broker: {"queue2": queue_config2},
        queue_config3.broker: {"queue3": queue_config3},
    }
    return DefaultConfigStore(
        queue_config=queue_config, broker_queue_configs=broker_queue_configs
    )


@pytest.fixture()
def hybrid_queue_configs_store(
    queue_config, queue_config2, queue_config3, queue_config4
):
    assert queue_config.broker is queue_config2.broker
    assert queue_config3.broker is not queue_config.broker
    assert queue_config3.broker is queue_config4.broker
    broker_queue_config = {
        queue_config.broker: queue_config,
        queue_config3.broker: queue_config3,
    }
    broker_queue_configs = {
        queue_config2.broker: {"queue2": queue_config2},
        queue_config3.broker: {"queue3": queue_config4},
    }
    return DefaultConfigStore(
        queue_config=queue_config,
        broker_queue_config=broker_queue_config,
        broker_queue_configs=broker_queue_configs,
        queue_names_by_broker={
            queue_config2.broker: ["queue2"],
            queue_config3.broker: ["queue3", "queue3-2"],
        }
    )
