"""Signed-cookie session auth with database users and bcrypt."""
from __future__ import annotations

from itsdangerous import URLSafeTimedSerializer, BadSignature, SignatureExpired
from passlib.hash import bcrypt

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from starlette.requests import Request
from starlette.responses import RedirectResponse

from app.config import settings

SESSION_COOKIE = "fenix_session"
SESSION_MAX_AGE = 86400  # 24 hours

_signer = URLSafeTimedSerializer(settings.secret_key)

# Plan limits
PLAN_LIMITS = {
    "free": {"searches": 50, "exports": 5, "watchlist": 10, "scoring": False, "enrichment": False},
    "pro": {"searches": -1, "exports": 50, "watchlist": 100, "scoring": True, "enrichment": True},
    "enterprise": {"searches": -1, "exports": -1, "watchlist": -1, "scoring": True, "enrichment": True},
}


def hash_password(password: str) -> str:
    return bcrypt.hash(password)


def verify_password(password: str, hashed: str) -> bool:
    return bcrypt.verify(password, hashed)


def create_session(user_id: int, email: str, role: str = "user", plan: str = "free") -> str:
    """Create a signed session token with user info."""
    return _signer.dumps({"user_id": user_id, "email": email, "role": role, "plan": plan})


def get_session_data(token: str | None) -> dict | None:
    """Extract session data from token. Returns None if invalid."""
    if not token:
        return None
    try:
        return _signer.loads(token, max_age=SESSION_MAX_AGE)
    except (BadSignature, SignatureExpired):
        return None


def is_authenticated(request: Request) -> bool:
    """Check if the current request has a valid session."""
    token = request.cookies.get(SESSION_COOKIE)
    return get_session_data(token) is not None


def get_current_user(request: Request) -> dict | None:
    """Get current user data from session. Returns dict with user_id, email, role, plan."""
    token = request.cookies.get(SESSION_COOKIE)
    return get_session_data(token)


async def authenticate_user(email: str, password: str, db: AsyncSession):
    """Authenticate user against database. Returns User or None."""
    from app.db.models import User
    result = await db.execute(select(User).where(User.email == email, User.is_active == True))
    user = result.scalar_one_or_none()
    if user and verify_password(password, user.password_hash):
        return user
    return None


async def create_user(email: str, nombre: str, password: str, db: AsyncSession, empresa: str = None, role: str = "user", plan: str = "free"):
    """Create a new user. Returns User or raises."""
    from app.db.models import User
    user = User(
        email=email.lower().strip(),
        nombre=nombre.strip(),
        empresa=empresa,
        password_hash=hash_password(password),
        role=role,
        plan=plan,
    )
    db.add(user)
    await db.commit()
    await db.refresh(user)
    return user


async def seed_default_users(db: AsyncSession):
    """Create admin and demo users if they don't exist."""
    from app.db.models import User
    for email, nombre, password, role, plan in [
        ("pedro@fenixia.tech", "Pedro", settings.admin_password, "admin", "enterprise"),
        ("demo@fenixia.tech", "Demo", settings.demo_password, "user", "free"),
    ]:
        existing = await db.scalar(select(User).where(User.email == email))
        if not existing:
            await create_user(email, nombre, password, db, role=role, plan=plan)
