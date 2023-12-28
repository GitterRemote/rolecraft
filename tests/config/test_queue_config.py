import dataclasses
import pytest


def test_to_queue_config(incomplete_queue_config, broker, queue_config):
    with pytest.raises(Exception):
        incomplete_queue_config.to_queue_config()


def test_to_queue_config_with_added_brorker(
    incomplete_queue_config, broker, queue_config
):
    config = incomplete_queue_config.replace(broker=broker)
    assert config.to_queue_config() == queue_config
