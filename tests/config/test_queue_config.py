import dataclasses
import pytest


def test_to_queue_config(incomplete_queue_config, broker, queue_config):
    with pytest.raises(Exception):
        incomplete_queue_config.to_queue_config()
    config = dataclasses.replace(incomplete_queue_config, broker=broker)
    assert config.to_queue_config() == queue_config
