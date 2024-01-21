from unittest import mock

import pytest

from rolecraft import middlewares as middlewares_mod
from rolecraft.queue_factory import queue_builder as queue_builder_mod


@pytest.fixture()
def queue_factory(cached_queue_factory):
    return cached_queue_factory


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
    # cached key is (queue_name, broker), so it will return the same queue even with a different encoder
    assert queue2.encoder is not encoder2
    assert queue2.encoder is queue.encoder
    assert queue2 is queue


def test_build_queue_with_cache(queue_factory, broker2, encoder2):
    def build_queue(queue_name, *args, **kwargs):
        q = mock.MagicMock()
        q.name = queue_name
        return q

    with mock.patch.object(
        queue_builder_mod.QueueBuilder, "build_queue", side_effect=build_queue
    ) as build_method:
        queue = queue_factory.build_queue(
            queue_name="queue1",
            broker=broker2,
            encoder=encoder2,
            middlewares=[middlewares_mod.Retryable()],
        )
        assert queue
        assert build_method.call_count == 1
        queue2 = queue_factory.build_queue(
            queue_name="queue1",
            broker=broker2,
            encoder=encoder2,
            middlewares=[middlewares_mod.Retryable()],
        )
        assert queue2 is queue
        assert build_method.call_count == 1

        queue3 = queue_factory.build_queue(
            queue_name="queue1",
            broker=broker2,
            encoder=encoder2,
            middlewares=[],
        )
        assert queue3 is queue
        assert build_method.call_count == 1

        queue4 = queue_factory.build_queue(
            queue_name="queue2",
            broker=broker2,
            encoder=encoder2,
            middlewares=[],
        )
        assert queue4 is not queue
        assert build_method.call_count == 2


def test_clear(queue_factory):
    def build_queue(queue_name, *args, **kwargs):
        q = mock.MagicMock()
        q.name = queue_name
        return q

    with mock.patch.object(
        queue_builder_mod.QueueBuilder, "build_queue", side_effect=build_queue
    ) as build_method:
        queue = queue_factory.build_queue(queue_name="queue1")
        assert queue
        assert build_method.call_count == 1

        queue2 = queue_factory.build_queue(queue_name="queue1")
        assert queue2 is queue
        assert build_method.call_count == 1

        queue_factory.clear()

        queue3 = queue_factory.build_queue(queue_name="queue1")
        assert queue3 is not queue
        assert build_method.call_count == 2
