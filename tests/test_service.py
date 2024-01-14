import contextlib
import threading
import time
from typing import Unpack

import pytest

import rolecraft
from rolecraft import broker as broker_mod
from rolecraft import role


@pytest.fixture
def broker():
    return broker_mod.StubBroker()


@pytest.fixture
def broker2():
    return broker_mod.StubBroker()


@pytest.fixture(autouse=True)
def config(broker):
    config = rolecraft.Config().set_default(broker=broker)
    config.inject()
    return config


@pytest.fixture(autouse=True)
def clear_role_decorator():
    yield

    # TODO: add clear method to role decorator
    role.role_hanger.clear()
    # If the queue factory doesn't clear its cached queue, then the broker will be different from the service' broker, which is a brand new broker object.
    clear_method = getattr(role.queue_factory, "clear", None)
    if clear_method:
        clear_method()


@pytest.fixture(params=[1, 2, 3], ids=["1 worker", "2 workers", "3 workers"])
def thread_num(request):
    return request.param


@pytest.fixture(
    params=[1, 10, 1000], ids=["prefetch 1", "prefetch 10", "prefetch 1000"]
)
def prefetch_size(request):
    return request.param


@pytest.fixture()
def create_service(thread_num, prefetch_size):
    @contextlib.contextmanager
    def _create_service(
        thread_num: int = thread_num,
        **service_opions: Unpack[rolecraft.ServiceCreateOptions],
    ):
        service_opions["prefetch_size"] = prefetch_size

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


def test_dispatch_messages(create_service):
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
    assert set(map(lambda x: x * 2, range(100))) == set(rv)


def test_dispatch_messages_with_multiple_queues(create_service):
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


def test_dispatch_messages_with_defined_queue_name(create_service):
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


def test_dispatch_messages_with_role_bound_broker(create_service, broker2):
    rv = []

    @role(broker=broker2)
    def fn(first: int, *, second: int):
        rv.append(first + second)

    with create_service():
        for i in range(100):
            fn.dispatch_message(i, second=i)
        time.sleep(0.1)

    assert not rv

    with create_service(queue_names_by_broker={broker2: ["default"]}):
        for i in range(100):
            fn.dispatch_message(i, second=i)
        time.sleep(0.1)
    assert len(rv) == 200
    assert set(map(lambda x: x * 2, range(100))) == set(rv)


def test_dispatch_messages_fail_retry(config, create_service):
    config.queue_config.middlewares.retryable.max_backoff_millis = 0
    config.inject()

    rv = []

    @role
    def fn(first: int, *, second: int):
        rv.append(False)
        raise RuntimeError("fn fails")

    with create_service():
        fn.dispatch_message(0, second=1)
        time.sleep(0.1)
    assert len(rv) == 4
    assert rv == [False] * 4


def test_dispatch_messages_hard_fail(config, create_service):
    class HardFailureError(Exception):
        ...

    config.queue_config.middlewares.retryable.max_backoff_millis = 0
    config.queue_config.middlewares.retryable.raises = HardFailureError
    config.inject()

    rv = []

    @role
    def fn(first: int, *, second: int):
        rv.append(False)
        raise HardFailureError

    with create_service():
        fn.dispatch_message(0, second=1)
        time.sleep(0.1)
    assert len(rv) == 1
    assert rv == [False]
