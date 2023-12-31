from unittest import mock

import pytest

from rolecraft.role_lib import role_hanger as role_hanger_mod


@pytest.fixture
def role():
    mocked = mock.MagicMock()
    mocked.name = "MockedRole"
    return mocked


@pytest.fixture
def role_hanger():
    return role_hanger_mod.RoleHanger()


def test_put(role_hanger, role):
    role_hanger.put(role)
    assert role_hanger.get(role.name) is role


def test_iter(role_hanger, role):
    assert list(role_hanger) == []
    
    role_hanger.put(role)
    assert next(iter(role_hanger)) is role
    assert list(role_hanger) == [role]
