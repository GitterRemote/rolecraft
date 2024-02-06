import dataclasses
from unittest import mock

import pytest

from rolecraft.config import config_store as config_store_mod

IncompleteConfigError = config_store_mod.IncompleteConfigError

DefaultConfigStore = config_store_mod.SimpleConfigStore


def assert_queue_config_eq(incomplete_queue_config, queue_config):
    for field in dataclasses.fields(incomplete_queue_config):
        assert getattr(incomplete_queue_config, field.name) == getattr(
            queue_config, field.name
        ), field.name


class TestEmptyStore:
    @pytest.fixture()
    def store(self, incomplete_queue_config):
        return DefaultConfigStore(queue_config=incomplete_queue_config)

    def test_fetch_default_config(self, store):
        with pytest.raises(IncompleteConfigError):
            store()

    def test_fetch_any_queue_config(self, store):
        with pytest.raises(IncompleteConfigError):
            store("any_queue_name")

    def test_fetch_any_broker_queue_config(self, store, broker):
        config = store(broker=broker)

        assert config.broker is broker

        assert_queue_config_eq(
            store.queue_config.replace(broker=broker), config
        )

    def test_fetch_queue_config_with_queue_and_broker(self, store, broker):
        config = store("any_queue_name", broker=broker)

        assert config.broker is broker
        assert_queue_config_eq(
            store.queue_config.replace(broker=broker), config
        )

    def test_fetch_queue_config_with_encoder(self, store, encoder):
        with pytest.raises(IncompleteConfigError):
            store(encoder=encoder)

        with pytest.raises(IncompleteConfigError):
            store("any_queue_name", encoder=encoder)

    def test_fetch_queue_config_with_encoder_and_broker(
        self, store, broker, encoder
    ):
        config = store(encoder=encoder, broker=broker)

        assert config.encoder is encoder
        assert config.broker is broker

        assert_queue_config_eq(
            store.queue_config.replace(broker=broker, encoder=encoder), config
        )

    def test_fetch_queue_config_with_middlewares(self, store, middlewares):
        with pytest.raises(IncompleteConfigError):
            store(middlewares=middlewares)

    def test_fetch_queue_config_with_middlewares_and_broker(
        self, store, broker, middlewares
    ):
        config = store(middlewares=middlewares, broker=broker)

        assert config.middlewares is middlewares
        assert config.broker is broker

        assert_queue_config_eq(
            store.queue_config.replace(broker=broker, middlewares=middlewares),
            config,
        )


class TestBrokerQueueConfigOnlyStore:
    @pytest.fixture()
    def store(self, broker_queue_config_only_store):
        return broker_queue_config_only_store

    def test_fetch_queue_config(
        self, store, queue_config, queue_config2, queue_config3
    ):
        config = store(broker=queue_config.broker)
        assert config.broker is queue_config.broker
        assert config.encoder is queue_config.encoder
        assert config == queue_config

        config = store(broker=queue_config3.broker)
        assert config.broker is queue_config3.broker
        assert config.encoder is queue_config3.encoder
        assert config == queue_config3

        assert queue_config2.broker is queue_config.broker
        config = store("queue2", broker=queue_config2.broker)
        assert config.broker is queue_config.broker
        assert config.encoder is queue_config.encoder
        assert config == queue_config

        unknown_broker = mock.MagicMock()
        config = store(broker=unknown_broker)
        assert config.broker is unknown_broker
        assert config.encoder is queue_config.encoder
        assert config.replace(broker=queue_config.broker) == queue_config


class TestBrokerQueueConfigsOnlyStore:
    @pytest.fixture()
    def store(self, broker_queue_configs_only_store):
        return broker_queue_configs_only_store

    def test_fetch_config_with_broker(
        self,
        store,
        broker,
        broker2,
        queue_config,
        queue_config2,
        queue_config3,
    ):
        assert store(broker=broker) == queue_config
        assert store(broker=broker2) == queue_config.replace(broker=broker2)

        assert store("any_queue_name") == queue_config
        assert store("queue2") == queue_config2
        assert store("queue2", broker=broker2) != queue_config2
        assert store("queue2", broker=broker2) == queue_config.replace(
            broker=broker2
        )
        assert store("queue3") != queue_config3
        assert store("queue3") == queue_config
        assert store("queue3", broker=broker2) == queue_config3

    def test_parsed_queue_names_by_broker(
        self, store, queue_config2, queue_config3
    ):
        assert store.parsed_queue_names_by_broker() == {
            queue_config2.broker: ["queue2"],
            queue_config3.broker: ["queue3"],
        }


class TestHybridQueueConfigs:
    @pytest.fixture()
    def store(self, hybrid_queue_configs_store):
        return hybrid_queue_configs_store

    def test_parsed_queue_names_by_broker(
        self, store, queue_config2, queue_config3
    ):
        assert store.parsed_queue_names_by_broker() == {
            queue_config2.broker: ["queue2"],
            queue_config3.broker: ["queue3", "queue3-2"],
        }


class TestFetchQueueConfigWithParameters:
    @pytest.fixture(
        params=[
            "default_only_store",
            "broker_queue_config_only_store",
            "broker_queue_configs_only_store",
            "hybrid_queue_configs_store",
        ]
    )
    def store(self, request) -> DefaultConfigStore:
        return request.getfixturevalue(request.param)

    @pytest.fixture()
    def unknown_broker(self):
        return mock.MagicMock()

    def test_fetch_default_config(self, store, queue_config):
        assert store() is queue_config

    def test_fetch_any_queue_config(self, store, queue_config):
        assert store("any_queue_name") is queue_config

    def test_fetch_queue_config_of_the_default_broker(
        self, store, broker, queue_config
    ):
        config = store(broker=broker)

        assert queue_config == config

    def test_fetch_queue_config_of_the_another_broker(
        self, store, unknown_broker
    ):
        config = store(broker=unknown_broker)

        assert unknown_broker == config.broker
        assert store.queue_config.replace(broker=unknown_broker) == config

    def test_fetch_queue_config_with_queue_and_broker(
        self, store, unknown_broker
    ):
        config = store("any_queue_name", broker=unknown_broker)

        assert config.broker is unknown_broker
        assert store.queue_config.replace(broker=unknown_broker) == config

    def test_fetch_queue_config_with_encoder(self, store, encoder):
        config = store("any_queue_name", encoder=encoder)

        assert config.encoder is encoder
        assert store.queue_config.replace(encoder=encoder) == config

    def test_fetch_queue_config_with_encoder_and_broker(
        self, store, broker, encoder
    ):
        config = store(encoder=encoder, broker=broker)

        assert config.encoder is encoder
        assert config.broker is broker
        assert (
            store.queue_config.replace(broker=broker, encoder=encoder)
            == config
        )

    def test_fetch_queue_config_with_middlewares(self, store, middlewares):
        config = store("any_queue_name", middlewares=middlewares)

        assert config.middlewares is middlewares
        assert store.queue_config.replace(middlewares=middlewares) == config

    def test_fetch_queue_config_with_middlewares_and_broker(
        self, store, broker, middlewares
    ):
        config = store(middlewares=middlewares, broker=broker)

        assert config.middlewares is middlewares
        assert config.broker is broker
        assert (
            store.queue_config.replace(broker=broker, middlewares=middlewares)
            == config
        )
