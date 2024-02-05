import dataclasses
import threading
from unittest import mock

import pytest

from rolecraft.queue import MessageQueue
from rolecraft.service.consumer import (
    threaded_consumer as threaded_consumer_mod,
)
from rolecraft.service.consumer.threaded_consumer import ThreadedConsumer


@pytest.fixture()
def threaded_consumer(queues):
    c = threaded_consumer_mod.ThreadedConsumer(queues=queues, prefetch_size=1)
    c.start()
    yield c
    c.stop()
    c.join()


@pytest.fixture()
def consumer(threaded_consumer):
    yield threaded_consumer


class TestWithMockedQueue:
    def create_future(self, queue, *rv, timeout=0):
        @dataclasses.dataclass
        class Message:
            id: str

            def requeue(self, *args, **kwargs):
                return queue.requeue(self, *args, **kwargs)

        class ResultFuture:
            def __init__(self, *rv, timeout=0) -> None:
                self.rv = list(rv)
                self.ev = threading.Event()
                self.timeout = timeout

            def result(self):
                if not self.timeout:
                    return self.rv
                if not self.ev.wait(self.timeout):
                    return self.rv
                return []

            def cancel(self):
                self.ev.set()

        rv = map(Message, map(str, rv))

        return ResultFuture(*rv, timeout=timeout)

    @pytest.fixture()
    def result_future(self):
        return self.create_future(1)

    @pytest.fixture()
    def queue(self, result_future):
        q = mock.MagicMock(MessageQueue)
        q.name = "MockedQueue"

        q.block_receive.side_effect = [
            self.create_future(q, str(i)) for i in range(1, 101)
        ]
        yield q

    @pytest.fixture()
    def queues(self, queue):
        yield [queue]

    def test_consume(self, consumer: ThreadedConsumer, queue):
        rv = []

        def consume():
            msgs = consumer.consume()
            rv.append(msgs)

        threads = []
        for _ in range(2):
            t = threading.Thread(target=consume)
            t.start()
            threads.append(t)

        for t in threads:
            t.join()
        assert len(rv) == 2
        for idx, msgs in enumerate(rv):
            assert len(msgs) == 1
            assert msgs[0].id == str(idx + 1)

        consumer.stop()
        consumer.join()
        assert queue.block_receive.call_count >= 2
        assert queue.requeue.call_count == queue.block_receive.call_count - 2
