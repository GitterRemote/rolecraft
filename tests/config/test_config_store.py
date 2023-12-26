import dataclasses
import pytest
from unittest import mock
from rolecraft.config import config_store as config_store_mod

IncompleteConfigError = config_store_mod.IncompleteConfigError

DefaultConfigStore = config_store_mod.DefaultConfigStore


class TestEmptyStore:
    @pytest.fixture()
    def store(self, incomplete_queue_config):
        return DefaultConfigStore(queue_config=incomplete_queue_config)

    def test_set_default(self, store):
        store.set_as_defaut()
        assert DefaultConfigStore.get() is store

    def test_fetch_default_config(self, store):
        with pytest.raises(IncompleteConfigError):
            store()

    def test_fetch_any_queue_config(self, store):
        with pytest.raises(IncompleteConfigError):
            store("any_queue_name")

    def test_fetch_any_broker_queue_config(self, store, broker):
        config = store(broker=broker)

        assert config.broker is broker
        assert (
            dataclasses.replace(
                store.queue_config, broker=broker
            ).to_queue_config()
            == config
        )

    def test_fetch_queue_config_with_queue_and_broker(self, store, broker):
        config = store("any_queue_name", broker=broker)

        assert config.broker is broker
        assert (
            dataclasses.replace(
                store.queue_config, broker=broker
            ).to_queue_config()
            == config
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
        assert (
            dataclasses.replace(
                store.queue_config,
                broker=broker,
                encoder=encoder,
            ).to_queue_config()
            == config
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
        assert (
            dataclasses.replace(
                store.queue_config,
                broker=broker,
                middlewares=middlewares,
            ).to_queue_config()
            == config
        )


class TestQueueConfigsOnlyStore:
    @pytest.fixture()
    def store(self, queue_configs_only_store):
        return queue_configs_only_store

    def test_fetch_queue_config_for_queue2(self, store, queue_config2):
        assert store("queue2") is queue_config2


class TestBrokerQueueConfigOnlyStore:
    @pytest.fixture()
    def store(self, broker_queue_configs_only_store):
        return broker_queue_configs_only_store

    def test_queue_to_broker(
        self, store, broker, broker2, queue_config, queue_config4
    ):
        def queue_to_broker(queue_name: str):
            if queue_name == "a_queue_with_broker2":
                return broker2
            return broker

        store.queue_to_broker = queue_to_broker
        assert store("any_queue_name") == queue_config
        assert store("a_queue_with_broker2") == queue_config4

    def test_fetch_config_with_broker(
        self, store, broker2, queue_config, queue_config4
    ):
        assert store("any_queue_name") == queue_config
        assert store("any_queue_name", broker=broker2) == queue_config4


class TestFetchQueueConfigWithParameters:
    @pytest.fixture(
        params=[
            "default_only_store",
            "queue_configs_only_store",
            "broker_queue_configs_only_store",
            "hybrid_queue_configs__store",
        ]
    )
    def store(self, request) -> DefaultConfigStore:
        return request.getfixturevalue(request.param)

    def test_fetch_default_config(self, store, queue_config):
        assert store() is queue_config

    def test_fetch_any_queue_config(self, store, queue_config):
        assert store("any_queue_name") is queue_config

    def test_fetch_queue_config_of_the_default_broker(
        self, store, broker, queue_config
    ):
        config = store(broker=broker)

        assert queue_config == config

    def test_fetch_queue_config_of_the_another_broker(self, store, broker2):
        config = store(broker=broker2)

        assert broker2 == config.broker
        assert (
            dataclasses.replace(store.queue_config, broker=broker2) == config
        )

    def test_fetch_queue_config_with_queue_and_broker(self, store, broker2):
        config = store("any_queue_name", broker=broker2)

        assert config.broker is broker2
        assert (
            dataclasses.replace(store.queue_config, broker=broker2) == config
        )

    def test_fetch_queue_config_with_encoder(self, store, encoder):
        config = store("any_queue_name", encoder=encoder)

        assert config.encoder is encoder
        assert (
            dataclasses.replace(store.queue_config, encoder=encoder) == config
        )

    def test_fetch_queue_config_with_encoder_and_broker(
        self, store, broker, encoder
    ):
        config = store(encoder=encoder, broker=broker)

        assert config.encoder is encoder
        assert config.broker is broker
        assert (
            dataclasses.replace(
                store.queue_config,
                broker=broker,
                encoder=encoder,
            )
            == config
        )

    def test_fetch_queue_config_with_middlewares(self, store, middlewares):
        config = store("any_queue_name", middlewares=middlewares)

        assert config.middlewares is middlewares
        assert (
            dataclasses.replace(store.queue_config, middlewares=middlewares)
            == config
        )

    def test_fetch_queue_config_with_middlewares_and_broker(
        self, store, broker, middlewares
    ):
        config = store(middlewares=middlewares, broker=broker)

        assert config.middlewares is middlewares
        assert config.broker is broker
        assert (
            dataclasses.replace(
                store.queue_config,
                broker=broker,
                middlewares=middlewares,
            )
            == config
        )

    @pytest.fixture
    def unused_broker(self):
        return mock.MagicMock()

    def test_queue_to_unconfigured_broker(
        self, store, unused_broker, queue_config
    ):
        def queue_to_broker(queue_name: str):
            return unused_broker

        store.queue_to_broker = queue_to_broker
        assert store("any_queue_name") == queue_config
