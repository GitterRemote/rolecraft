import pytest

from rolecraft import middlewares as middlewares_mod
from rolecraft.middleware import Middleware
from rolecraft.queue_factory import queue_factory as queue_factory_mod


@pytest.fixture()
def queue_factory(config_fetcher):
    return queue_factory_mod.QueueFactory(config_fetcher=config_fetcher)


def test_build_queue(queue_factory, queue_config):
    queue = queue_factory.build_queue(queue_name="queue1")
    assert queue
    assert queue.name == "queue1"
    assert queue.broker is queue_config.broker
    assert queue.encoder is queue_config.encoder
    assert queue.wait_time_seconds == queue_config.wait_time_seconds


def test_build_queue_with_params(
    queue_factory, queue_config, broker2, encoder2
):
    queue = queue_factory.build_queue(queue_name="queue1", broker=broker2)
    assert queue.name == "queue1"
    assert queue.broker is broker2
    assert queue.encoder is queue_config.encoder

    queue2 = queue_factory.build_queue(
        queue_name="queue1", broker=broker2, encoder=encoder2
    )
    assert queue2.encoder is encoder2


def test_build_queue_with_middlewares(queue_factory, broker2, encoder2):
    queue = queue_factory.build_queue(
        queue_name="queue1",
        broker=broker2,
        encoder=encoder2,
        middlewares=[middlewares_mod.Retryable()],
    )
    assert isinstance(queue, Middleware)
    assert queue.name == "queue1"
    assert queue.broker is broker2
    assert queue.encoder is encoder2


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
