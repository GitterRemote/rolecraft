import pytest
from rolecraft.config import configurable_config as configurable_config_mod
from rolecraft.config import config_store as config_store_mod

IncompleteConfigError = config_store_mod.IncompleteConfigError


@pytest.fixture(params=["incomplete", "complete"])
def configurable_config(request, incomplete_queue_config, queue_config):
    if request.param == "incomplete":
        return configurable_config_mod.ConfigurableConfig(
            queue_config=incomplete_queue_config
        )
    else:
        return configurable_config_mod.ConfigurableDefaultConfig(
            queue_config=queue_config
        )


def test_add_broker_config(configurable_config, broker, encoder, middlewares):
    configurable_config.add_broker_config(
        broker, encoder=encoder, middlewares=middlewares
    )

    store = configurable_config.create_config_store()
    queue_config = store.fetcher(broker=broker)
    assert queue_config.broker is broker
    assert queue_config.encoder is encoder
    assert queue_config.middlewares is middlewares
    assert (
        queue_config.wait_time_seconds
        == configurable_config.queue_config.wait_time_seconds
    )


def test_add_queue_config(configurable_config, broker, encoder, middlewares):
    configurable_config.add_queue_config(
        "queue1", broker=broker, encoder=encoder, middlewares=middlewares
    )

    store = configurable_config.create_config_store()
    queue_config = store.fetcher("queue1")
    assert queue_config.broker is broker
    assert queue_config.encoder is encoder
    assert queue_config.middlewares is middlewares


def test_broker_add_queue_config(
    configurable_config, broker, encoder, middlewares, encoder2
):
    configurable_config.add_queue_config(
        "queue1", broker=broker, encoder=encoder, middlewares=middlewares
    )
    broker_config = configurable_config.add_broker_config(
        broker, encoder=encoder, middlewares=[]
    )
    broker_config.add_queue_config("queue2", encoder=encoder2)

    # queue1 config is unchanged
    store = configurable_config.create_config_store()
    queue1_config = store.fetcher("queue1")
    assert queue1_config.broker is broker
    assert queue1_config.encoder is encoder
    assert queue1_config.middlewares is middlewares

    # queue2 config is based on the new broker config
    queue2_config = store.fetcher("queue2")
    assert queue2_config.broker is broker
    assert queue2_config.encoder is encoder2
    assert queue2_config.middlewares == []


def test_queue_names_broker_mapping(
    configurable_config, broker, encoder, middlewares
):
    configurable_config.add_broker_config(
        broker, encoder=encoder, middlewares=middlewares
    )
    configurable_config.queue_names_by_broker = {broker: ["queue1", "queue2"]}

    store = configurable_config.create_config_store()
    queue_config = store.fetcher("queue1")
    assert queue_config.broker is broker
    assert queue_config.encoder is encoder


def test_queue_to_broker_callable(
    configurable_config, broker, encoder, middlewares
):
    configurable_config.add_broker_config(
        broker, encoder=encoder, middlewares=middlewares
    )

    def queue_to_broker(queue_name: str):
        if queue_name == "queue1":
            return broker
        return None

    configurable_config.queue_to_broker = queue_to_broker

    store = configurable_config.create_config_store()
    queue_config = store.fetcher("queue1")
    assert queue_config.broker is broker
    assert queue_config.encoder is encoder


class TestIncompleteConfig:
    @pytest.fixture
    def configurable_config(self, incomplete_queue_config):
        return configurable_config_mod.ConfigurableConfig(
            queue_config=incomplete_queue_config
        )

    def test_update_default(self, configurable_config, broker):
        config = configurable_config.update_default(broker=broker)
        assert config.queue_config.broker is broker

    def test_update_default_keep_original_data(
        self, configurable_config, broker, broker2, encoder
    ):
        configurable_config.add_queue_config(
            "queue1", broker=broker2, encoder=encoder
        )
        config = configurable_config.update_default(broker=broker)
        assert config.queue_config.broker is broker

        store = configurable_config.create_config_store()
        queue_config = store.fetcher("queue1")
        assert queue_config.broker is broker2

    def test_add_broker_config(
        self, configurable_config, broker, encoder, middlewares
    ):
        configurable_config.add_broker_config(
            broker, encoder=encoder, middlewares=middlewares
        )

        store = configurable_config.create_config_store()

        with pytest.raises(IncompleteConfigError):
            store.fetcher()

    def test_add_queue_config(
        self, configurable_config, broker, encoder, middlewares
    ):
        configurable_config.add_queue_config(
            "queue1", broker=broker, encoder=encoder, middlewares=middlewares
        )

        store = configurable_config.create_config_store()

        with pytest.raises(IncompleteConfigError):
            store.fetcher()

        with pytest.raises(IncompleteConfigError):
            store.fetcher("queue2")

    def test_queue_names_broker_mapping(
        self, configurable_config, broker, encoder, middlewares, encoder2
    ):
        configurable_config.add_broker_config(
            broker, encoder=encoder, middlewares=middlewares
        )
        configurable_config.queue_names_by_broker = {
            broker: ["queue1", "queue2"]
        }

        store = configurable_config.create_config_store()

        with pytest.raises(IncompleteConfigError):
            store.fetcher("queue3")

    def test_queue_to_broker_callable(
        self, configurable_config, broker, encoder, middlewares
    ):
        configurable_config.add_broker_config(
            broker, encoder=encoder, middlewares=middlewares
        )

        def queue_to_broker(queue_name: str):
            if queue_name == "queue1":
                return broker
            return None

        configurable_config.queue_to_broker = queue_to_broker

        store = configurable_config.create_config_store()

        with pytest.raises(IncompleteConfigError):
            store.fetcher("queue2")
