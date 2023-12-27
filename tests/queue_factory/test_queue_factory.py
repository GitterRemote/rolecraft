import pytest
from unittest import mock
from rolecraft.queue_factory import queue_factory as queue_factory_mod
from rolecraft.queue_factory import queue_builder as queue_builder_mod
from rolecraft import queue_config as queue_config_mod
import dataclasses
from rolecraft import middleware as middleware_mod


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
        return dataclasses.replace(queue_config, **kwargs)

    return fetch


@pytest.fixture()
def queue_factory(config_fetcher):
    return queue_factory_mod.QueueFactory(config_fetcher=config_fetcher)


def test_get_and_build(queue_factory, queue_config):
    queue = queue_factory.get_or_build(queue_name="queue1")
    assert queue
    assert queue.name == "queue1"
    assert queue.broker is queue_config.broker
    assert queue.encoder is queue_config.encoder
    assert queue.wait_time_seconds == queue_config.consumer_wait_time_seconds


def test_get_and_build_with_params(
    queue_factory, queue_config, broker2, encoder2
):
    queue = queue_factory.get_or_build(queue_name="queue1", broker=broker2)
    assert queue.name == "queue1"
    assert queue.broker is broker2
    assert queue.encoder is queue_config.encoder

    queue = queue_factory.get_or_build(
        queue_name="queue1", broker=broker2, encoder=encoder2
    )
    assert queue.encoder is encoder2


def test_get_and_build_with_middlewares(queue_factory, broker2, encoder2):
    queue = queue_factory.get_or_build(
        queue_name="queue1",
        broker=broker2,
        encoder=encoder2,
        middlewares=[middleware_mod.Retryable()],
    )
    assert isinstance(queue, middleware_mod.Middleware)
    assert queue.name == "queue1"
    assert queue.broker is broker2
    assert queue.encoder is encoder2


def test_get_and_build_cache(queue_factory, broker2, encoder2):
    with mock.patch.object(
        queue_builder_mod.QueueBuilder, "build_one"
    ) as build_method:
        queue = queue_factory.get_or_build(
            queue_name="queue1",
            broker=broker2,
            encoder=encoder2,
            middlewares=[middleware_mod.Retryable()],
        )
        assert queue
        assert build_method.call_count == 1
        queue2 = queue_factory.get_or_build(
            queue_name="queue1",
            broker=broker2,
            encoder=encoder2,
            middlewares=[middleware_mod.Retryable()],
        )
        assert queue2 is queue
        # As that Retryable doesn't implement __eq__, so the build_method will be call again
        assert middleware_mod.Retryable() != middleware_mod.Retryable()
        assert build_method.call_count == 2

    retryable = middleware_mod.Retryable()
    with mock.patch.object(
        queue_builder_mod.QueueBuilder, "build_one"
    ) as build_method:
        queue = queue_factory.get_or_build(
            queue_name="queue1",
            broker=broker2,
            encoder=encoder2,
            middlewares=[retryable],
        )
        assert queue
        assert build_method.call_count == 1
        queue2 = queue_factory.get_or_build(
            queue_name="queue1",
            broker=broker2,
            encoder=encoder2,
            middlewares=[retryable],
        )
        assert queue2 is queue
        assert build_method.call_count == 1


def test_build_queues_empty(queue_factory, queue_config):
    assert not queue_factory.build_queues()


def test_build_queues_with_queue_names(queue_factory, queue_config):
    queues = queue_factory.build_queues(queue_names=["queue1"])
    assert len(queues) == 1
    queue = queues[0]
    assert queue.name == "queue1"
    assert queue.broker is queue_config.broker
    assert queue.encoder is queue_config.encoder

    queues = queue_factory.build_queues(queue_names=["queue2", "queue1"])
    assert len(queues) == 2
    queue = queues[1]
    assert queue.name == "queue1"
    assert queue.broker is queue_config.broker
    assert queue.encoder is queue_config.encoder

    queue2 = queues[0]
    assert queue2.name == "queue2"


def test_build_queues_with_queue_names_by_broker(
    queue_factory, queue_config, broker2
):
    queues = queue_factory.build_queues(
        queue_names_by_broker={broker2: ["queue1"]}
    )

    assert len(queues) == 1
    queue = queues[0]
    assert queue.name == "queue1"
    assert queue.broker is broker2
    assert queue.encoder is queue_config.encoder

    queues = queue_factory.build_queues(
        queue_names=["queue2"],
        queue_names_by_broker={broker2: ["queue1", "queue3"]},
    )
    assert len(queues) == 3
    queue_names = set(q.name for q in queues)
    assert queue_names == {"queue1", "queue2", "queue3"}
    queue = next(filter(lambda q: q.name.endswith("1"), queues))
    assert queue.name == "queue1"
    assert queue.broker is broker2
    assert queue.encoder is queue_config.encoder

    queue2 = next(filter(lambda q: q.name.endswith("2"), queues))
    assert queue2.broker is queue_config.broker
