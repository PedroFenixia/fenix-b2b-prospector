"""Tests for web routes."""
from __future__ import annotations

import pytest
from app.auth import create_session, SESSION_COOKIE


@pytest.mark.asyncio
async def test_login_page(client):
    resp = await client.get("/login")
    assert resp.status_code == 200
    assert "FENIX" in resp.text


@pytest.mark.asyncio
async def test_login_redirects_when_unauthenticated(client):
    resp = await client.get("/", follow_redirects=False)
    assert resp.status_code == 302
    assert "/login" in resp.headers["location"]


@pytest.mark.asyncio
async def test_authenticated_access(client):
    token = create_session()
    client.cookies.set(SESSION_COOKIE, token)
    resp = await client.get("/", follow_redirects=False)
    assert resp.status_code == 200


@pytest.mark.asyncio
async def test_health_endpoint(client):
    resp = await client.get("/health")
    assert resp.status_code == 200
    data = resp.json()
    assert data["status"] == "ok"
