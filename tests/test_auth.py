"""Tests for authentication."""
from app.auth import create_session, validate_session, check_credentials
from app.config import settings


def test_create_and_validate_session():
    token = create_session()
    assert token is not None
    assert validate_session(token) is True


def test_validate_invalid_session():
    assert validate_session(None) is False
    assert validate_session("invalid-token") is False


def test_check_credentials_valid():
    assert check_credentials(settings.admin_user, settings.admin_password) is True


def test_check_credentials_invalid():
    assert check_credentials("wrong", "wrong") is False
    assert check_credentials(settings.admin_user, "wrong") is False
