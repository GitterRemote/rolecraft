import dataclasses
import pytest
from rolecraft.config import config_store as config_store_mod


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
        with pytest.raises(RuntimeError):
            store.default_queue_config

    def test_fetch_default_config(self, store):
        with pytest.raises(RuntimeError):
            store()

    def test_fetch_any_queue_config(self, store):
        with pytest.raises(RuntimeError):
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

    def test_fetch__queue_config_with_queue_and_broker(self, store, broker):
        config = store("any_queue_name", broker=broker)

        assert config.broker is broker
        assert (
            dataclasses.replace(
                store.queue_config, broker=broker
            ).to_queue_config()
            == config
        )
