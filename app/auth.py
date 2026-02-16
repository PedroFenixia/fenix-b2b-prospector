"""Simple session-based authentication."""
from __future__ import annotations

import hashlib
import secrets
from datetime import datetime, timedelta

from starlette.requests import Request
from starlette.responses import RedirectResponse

from app.config import settings

# In-memory session store (simple for single-process deployment)
_sessions: dict[str, datetime] = {}
SESSION_COOKIE = "fenix_session"
SESSION_TTL = timedelta(hours=24)


def _hash(value: str) -> str:
    return hashlib.sha256(f"{settings.secret_key}:{value}".encode()).hexdigest()


def create_session() -> str:
    """Create a new session token."""
    token = secrets.token_urlsafe(32)
    _sessions[token] = datetime.utcnow() + SESSION_TTL
    return token


def validate_session(token: str | None) -> bool:
    """Check if a session token is valid."""
    if not token:
        return False
    expiry = _sessions.get(token)
    if not expiry:
        return False
    if datetime.utcnow() > expiry:
        _sessions.pop(token, None)
        return False
    return True


def destroy_session(token: str | None) -> None:
    """Remove a session."""
    if token:
        _sessions.pop(token, None)


def check_credentials(username: str, password: str) -> bool:
    """Verify login credentials against config."""
    return username == settings.admin_user and password == settings.admin_password


def is_authenticated(request: Request) -> bool:
    """Check if the current request has a valid session."""
    token = request.cookies.get(SESSION_COOKIE)
    return validate_session(token)


def require_login(request: Request) -> RedirectResponse | None:
    """If not authenticated, return redirect to login. Otherwise None."""
    if is_authenticated(request):
        return None
    return RedirectResponse(url="/login", status_code=302)
