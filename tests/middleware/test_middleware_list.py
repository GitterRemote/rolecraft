import pytest
from unittest import mock
from rolecraft import middleware
from rolecraft import queue as queue_mod
from rolecraft import broker as broker_mod


@pytest.fixture()
def empty_middleware_list():
    return middleware.MiddlewareList()


@pytest.fixture()
def middleware_list(retryable):
    return middleware.MiddlewareList([retryable])


# @pytest.fixture()
# def broker():
#     return mock.MagicMock(broker_mod.Broker)


# @pytest.fixture()
# def queue(broker):
#     return queue_mod.Queue(name="default", broker=broker)


@pytest.fixture()
def retryable():
    return middleware.Retryable()


def test_update_non_existing_middleware(empty_middleware_list, retryable):
    with pytest.raises(ValueError):
        empty_middleware_list.retryable = retryable


def test_append_a_new_middleware(empty_middleware_list, retryable):
    empty_middleware_list.append(retryable)
    assert empty_middleware_list.retryable is retryable
    assert empty_middleware_list[0] is retryable
    assert empty_middleware_list[-1] is retryable
    assert len(empty_middleware_list) == 1


def test_len(empty_middleware_list):
    assert len(empty_middleware_list) == 0


def test_iter(middleware_list):
    assert len(list(middleware_list)) == 1


def test_modify_a_middleware(middleware_list):
    middleware_list.retryable = None
    assert len(middleware_list) == 0


def test_overwrite_a_slice(middleware_list):
    pass


def test_concatenate(middleware_list):
    pass
