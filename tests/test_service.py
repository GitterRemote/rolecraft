import contextlib
import threading
import time

import pytest

import rolecraft
from rolecraft import broker as broker_mod
from rolecraft import role


@pytest.fixture
def broker():
    return broker_mod.StubBroker()


@pytest.fixture(autouse=True)
def config(broker):
    rolecraft.Config().set_default(broker=broker).inject()


@pytest.fixture(autouse=True)
def clear_role_decorator():
    yield

    # TODO: add clear method to role decorator
    role.role_hanger.clear()
    # If the queue factory doesn't clear its cached queue, then the broker will be different from the service' broker, which is a brand new broker object.
    role.queue_factory.clear()


@pytest.fixture(params=[1, 2, 3], ids=["1 worker", "2 workers", "3 workers"])
def create_service(request):
    thread_num = request.param

    @contextlib.contextmanager
    def _create_service(
        thread_num: int = thread_num, queue_names: list[str] | None = None
    ):
        service_opions = {}
        service_opions["prefetch_size"] = 1
        if queue_names:
            service_opions["queue_names"] = queue_names

        service = rolecraft.ServiceFactory().create(**service_opions)

        t = threading.Thread(
            target=service.start,
            kwargs=dict(thread_num=thread_num, ignore_signal=True),
        )
        t.start()
        yield service
        service.stop()
        service.join()
        t.join()

    return _create_service


def test_dispatch_messages(create_service, broker):
    rv = []

    @role
    def fn(first: int, *, second: int):
        rv.append(first + second)

    with create_service():
        fn.dispatch_message(0, second=1)
        time.sleep(0.1)
    assert len(rv) == 1
    assert rv[0] == 1

    rv.clear()
    with create_service():
        for i in range(100):
            fn.dispatch_message(i, second=i)
        time.sleep(0.1)
    assert len(rv) == 100
    assert rv[0] == 0
    assert rv[-1] == 99 * 2


def test_dispatch_messages_with_multiple_queues(create_service, broker):
    rv = []
    rv2 = []

    @role(queue_name="queue1")
    def fn(first: int, *, second: int):
        rv.append(first + second)

    @role(queue_name="queue2")
    def fn2(first: int, *, second: int):
        rv2.append(first * second)

    with create_service():
        for i in range(100):
            fn.dispatch_message(i, second=i)
            fn2.dispatch_message(i, second=i)
        time.sleep(0.1)
    assert len(rv) == 100
    assert set(map(lambda x: x * 2, range(100))) == set(rv)
    assert len(rv2) == 100
    assert set(map(lambda x: x**2, range(100))) == set(rv2)


def test_dispatch_messages_with_defined_queue_name(create_service, broker):
    rv = []

    @role
    def fn(first: int, *, second: int):
        rv.append(first + second)

    with create_service(queue_names=["queue1", "queue2"]):
        for i in range(100):
            # send to queue "default", however, it is not consumed by the wokers
            fn.dispatch_message(i, second=i)
            # send to queue "queue2"
            fn.dispatch_message_ext(
                args=(i,), kwds=dict(second=i), queue_name="queue2"
            )
        time.sleep(0.1)

    assert len(rv) == 100
    assert set(map(lambda x: x * 2, range(100))) == set(rv)
