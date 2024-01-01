from unittest import mock

import pytest

from rolecraft import message as message_mod
from rolecraft import middleware as middleware_mod
from rolecraft import queue as queue_mod


@pytest.fixture()
def queue():
    q = mock.MagicMock(queue_mod.MessageQueue)
    q.name = "MockedQueue"

    def retry(message, *args, **kwargs):
        message.meta.retries = (message.meta.retries or 0) + 1
        return True

    q.retry.side_effect = retry
    q.nack.return_value = True
    return q


@pytest.fixture()
def retryable(queue):
    return middleware_mod.Retryable(queue)


@pytest.fixture()
def message():
    msg = mock.MagicMock(message_mod.Message)
    msg.meta = mock.MagicMock(message_mod.Meta)
    msg.meta.retries = None
    return msg


def test_retry(retryable, queue, message):
    exc = Exception()

    assert retryable.nack(message=message, exception=exc) is True
    queue.retry.assert_called_once_with(
        message, delay_millis=retryable.base_backoff_millis, exception=exc
    )

    assert retryable.nack(message=message, exception=exc) is True
    queue.retry.assert_called()
    delay_millis = queue.retry.call_args.kwargs.get("delay_millis")
    assert delay_millis > retryable.base_backoff_millis
    assert (
        delay_millis
        <= retryable.base_backoff_millis * retryable.exponential_factor
        + retryable.base_backoff_millis * retryable.jitter_range
    )

    assert retryable.nack(message=message, exception=exc) is True
    queue.retry.assert_called()

    assert retryable.nack(message=message, exception=exc) is True
    queue.nack.assert_called_once_with(message, exception=exc)


def test_max_backoff_millis(retryable, queue, message):
    retryable.max_backoff_millis = 1000

    assert retryable.nack(message=message, exception=Exception()) is True
    queue.retry.assert_called()
    delay_millis = queue.retry.call_args.kwargs.get("delay_millis")
    assert delay_millis == 1000


def test_should_retry(retryable, queue, message):
    rv = False
    error = Exception()

    def should_retry(msg, exc, retries):
        nonlocal rv
        rv = msg is message and exc is error and retries == 0
        return False

    retryable.should_retry = should_retry

    assert retryable.nack(message=message, exception=error) is True
    queue.nack.assert_called_once_with(message, exception=error)
    assert rv is True


def test_raises(retryable, queue, message):
    class MyError(Exception):
        ...

    retryable.raises = (MyError,)

    assert retryable.nack(message=message, exception=MyError()) is True
    queue.nack.assert_called()

    assert retryable.nack(message=message, exception=Exception()) is True
    queue.retry.assert_called()
