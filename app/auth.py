"""Signed-cookie session auth (works across multiple workers)."""
from __future__ import annotations

from itsdangerous import URLSafeTimedSerializer, BadSignature, SignatureExpired

from starlette.requests import Request
from starlette.responses import RedirectResponse

from app.config import settings

SESSION_COOKIE = "fenix_session"
SESSION_MAX_AGE = 86400  # 24 hours in seconds

_signer = URLSafeTimedSerializer(settings.secret_key)


def create_session() -> str:
    """Create a signed session token (stateless, works across workers)."""
    return _signer.dumps({"user": settings.admin_user})


def validate_session(token: str | None) -> bool:
    """Validate a signed session token."""
    if not token:
        return False
    try:
        _signer.loads(token, max_age=SESSION_MAX_AGE)
        return True
    except (BadSignature, SignatureExpired):
        return False


def destroy_session(token: str | None) -> None:
    """No-op for stateless sessions (cookie is deleted client-side)."""
    pass


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
