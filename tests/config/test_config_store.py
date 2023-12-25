import dataclasses
import pytest
from rolecraft.config import config_store as config_store_mod

IncompleteConfigError = config_store_mod.IncompleteConfigError


class TestEmptyStore:
    @pytest.fixture()
    def store(self, incomplete_queue_config):
        return config_store_mod.ConfigStore(
            queue_config=incomplete_queue_config
        )

    def test_set_default(self, store):
        assert config_store_mod.ConfigStore.get() is None
        store.set_as_defaut()
        assert config_store_mod.ConfigStore.get() is store

    def test_default_queue_config(self, store):
        with pytest.raises(IncompleteConfigError):
            store.default_queue_config

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


class TestDefaultOnlyStore:
    @pytest.fixture()
    def store(self, queue_config):
        return config_store_mod.ConfigStore(queue_config=queue_config)

    def test_set_default(self, store):
        assert config_store_mod.ConfigStore.get() is None
        store.set_as_defaut()
        assert config_store_mod.ConfigStore.get() is store

    def test_default_queue_config(self, store, queue_config):
        assert store.default_queue_config is queue_config

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
        self, store, another_broker
    ):
        config = store(broker=another_broker)

        assert another_broker == config.broker
        assert (
            dataclasses.replace(store.queue_config, broker=another_broker)
            == config
        )

    def test_fetch_queue_config_with_queue_and_broker(
        self, store, another_broker
    ):
        config = store("any_queue_name", broker=another_broker)

        assert config.broker is another_broker
        assert (
            dataclasses.replace(store.queue_config, broker=another_broker)
            == config
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


class TestQueueConfigOnlyStore:
    pass


class TestBrokerQueueConfigOnlyStore:
    pass
