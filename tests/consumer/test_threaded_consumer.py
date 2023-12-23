import dataclasses
import threading
import pytest
from unittest import mock
from rolecraft.consumer import threaded_consumer as threaded_consumer_mod
from rolecraft.consumer.threaded_consumer import ThreadedConsumer
from rolecraft.queue import MessageQueue


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
    def create_future(self, *rv, timeout=0):
        @dataclasses.dataclass
        class Message:
            id: str

        class ResultFuture:
            def __init__(self, *rv, timeout=0) -> None:
                self.rv = list(map(Message, map(str, rv)))
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

        return ResultFuture(*rv, timeout=timeout)

    @pytest.fixture()
    def result_future(self):
        return self.create_future(1)

    @pytest.fixture()
    def queue(self, result_future):
        q = mock.MagicMock(MessageQueue)
        q.name = "MockedQueue"

        def block_receive(self, *args, **kwargs):
            return self.create_future()

        q.block_receive.side_effect = [
            self.create_future(1),
            self.create_future(2),
            self.create_future(3),
            self.create_future(4),
        ]
        yield q

    @pytest.fixture()
    def queues(self, queue):
        yield [queue]

    def test_consume(self, consumer: ThreadedConsumer, queue: mock.MagicMock):
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
        # assert rv == [[1], [2]]
        # FIXME: fix Message.requeue
        # queue.requeue.assert_any_call(2)
