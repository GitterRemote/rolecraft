from unittest import mock

import pytest

from rolecraft import encoder as _encoder
from rolecraft import message as _message


@pytest.fixture()
def queue():
    return mock.MagicMock()


@pytest.fixture()
def meta():
    return {}


@pytest.fixture()
def message(queue, meta):
    return _message.Message(
        id="123",
        meta=meta,
        queue=queue,
        role_name="default_role",
        role_data="role_data",
    )


@pytest.fixture()
def header_bytes_encoder():
    return _encoder.HeaderBytesEncoder()


def test_encode(header_bytes_encoder, message, queue):
    raw_msg = header_bytes_encoder.encode(message)
    assert raw_msg.id == message.id
    assert not raw_msg.headers
    assert raw_msg.data
    assert isinstance(raw_msg.data, bytes)

    msg = header_bytes_encoder.decode(raw_msg, queue=queue)
    assert msg == message


def test_meta_with_value(header_bytes_encoder, message, queue):
    message.meta["retries"] = 1

    raw_msg = header_bytes_encoder.encode(message)
    assert raw_msg.id == message.id
    assert str(raw_msg.headers["retries"]) == "1"
    assert raw_msg.data
    assert isinstance(raw_msg.data, bytes)

    msg = header_bytes_encoder.decode(raw_msg, queue=queue)
    assert msg.meta["retries"] == 1
    assert msg == message
