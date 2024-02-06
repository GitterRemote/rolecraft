from unittest import mock

import pytest

from rolecraft.broker import IrrecoverableError, RecoverableError
from rolecraft.queue.message import Message
from rolecraft.middlewares import queue_recoverable as queue_recoverable_mod


@pytest.fixture()
def middleware(queue):
    return queue_recoverable_mod.QueueRecoverable(queue)


@pytest.fixture()
def new_message(queue):
    def _new_message(role_name: str = "test_role_name"):
        return Message(role_name=role_name, queue=queue)

    return _new_message


def test_recoverable_error_for_receive(queue, middleware, new_message):
    msgs = [new_message(), new_message()]
    queue.receive.__name__ = "receive"
    queue.receive.side_effect = [RecoverableError, msgs]
    assert callable(queue.receive)

    rv = middleware.receive()
    assert rv == msgs
    assert queue.receive.call_count == 2


def test_recoverable_error_for_ack(queue, middleware, new_message):
    queue.ack.__name__ = "ack"
    queue.ack.side_effect = [RecoverableError, None]
    assert callable(queue.ack)

    msg = new_message()
    rv = middleware.ack(msg)
    assert rv is None
    assert queue.ack.call_count == 2
    queue.ack.assert_has_calls([mock.call(msg), mock.call(msg)])


def test_irrecoverable_error(queue, middleware, new_message):
    queue.receive.side_effect = [IrrecoverableError, [new_message()]]
    assert callable(queue.receive)

    with pytest.raises(IrrecoverableError):
        middleware.receive()

    assert queue.receive.call_count == 1


def test_recoverable_error_exceeds_max_retries(queue, middleware, new_message):
    middleware.queue_retries = 2
    msgs = [new_message(), new_message()]
    queue.receive.__name__ = "receive"
    queue.receive.side_effect = [
        RecoverableError,
        RecoverableError,
        RecoverableError,
        msgs,
    ]
    assert callable(queue.receive)

    with pytest.raises(RecoverableError):
        middleware.receive()
    assert queue.receive.call_count == 3
