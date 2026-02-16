"""Tests for the EU number format filter."""
import importlib


def _get_filter():
    from app.web.routes import _format_eu
    return _format_eu


def test_format_integer():
    f = _get_filter()
    assert f(1234567) == "1.234.567"


def test_format_decimal():
    f = _get_filter()
    assert f(1234.56) == "1.234,56"


def test_format_none():
    f = _get_filter()
    assert f(None) == "-"


def test_format_zero():
    f = _get_filter()
    assert f(0) == "0"
