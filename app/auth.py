"""Signed-cookie session auth with database users and bcrypt."""
from __future__ import annotations

import asyncio
from functools import partial

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

# Plan limits (-1 = unlimited, 0 = disabled)
# Free: full search & detail access, no productivity tools
# Pro €29/mes: exports, watchlist, alerts, scoring, enrichment
# Enterprise €79/mes: unlimited everything + API
PLAN_LIMITS = {
    "free": {"searches": -1, "exports": 0, "watchlist": 0, "detail_views": -1, "alerts": 0, "scoring": False, "enrichment": False},
    "pro": {"searches": -1, "exports": 100, "watchlist": 50, "detail_views": -1, "alerts": -1, "scoring": True, "enrichment": True, "enrichment_limit": 50},
    "enterprise": {"searches": -1, "exports": -1, "watchlist": -1, "detail_views": -1, "alerts": -1, "scoring": True, "enrichment": True, "enrichment_limit": -1},
}

# Bcrypt rounds: 10 balances security and performance (OWASP minimum)
_bcrypt = bcrypt.using(rounds=10)


def hash_password(password: str) -> str:
    return _bcrypt.hash(password)


def verify_password(password: str, hashed: str) -> bool:
    return bcrypt.verify(password, hashed)


async def hash_password_async(password: str) -> str:
    """Hash password without blocking the event loop."""
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(None, hash_password, password)


async def verify_password_async(password: str, hashed: str) -> bool:
    """Verify password without blocking the event loop."""
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(None, verify_password, password, hashed)


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
    """Authenticate user against database. Returns User or None.

    Uses constant-time comparison to prevent timing-based user enumeration.
    """
    from app.db.models import User
    # Valid bcrypt hash for constant-time comparison when user doesn't exist
    _DUMMY_HASH = "$2b$10$K4Gx7vFhS3Lq9p0jR1mN2OuX5c8d6e7f0g1h2i3j4k5l6m7n8o9p0q"
    result = await db.execute(select(User).where(User.email == email.lower().strip(), User.is_active == True))
    user = result.scalar_one_or_none()
    if user:
        if await verify_password_async(password, user.password_hash):
            return user
    else:
        # Always run bcrypt to prevent timing attacks
        await verify_password_async(password, _DUMMY_HASH)
    return None


async def create_user(
    email: str, nombre: str, password: str, db: AsyncSession,
    empresa: str = None, empresa_cif: str = None, telefono: str = None,
    role: str = "user", plan: str = "free",
):
    """Create a new user. Returns User or raises."""
    from app.db.models import User
    user = User(
        email=email.lower().strip(),
        nombre=nombre.strip(),
        empresa=empresa or "",
        empresa_cif=empresa_cif,
        telefono=telefono,
        password_hash=await hash_password_async(password),
        role=role,
        plan=plan,
    )
    db.add(user)
    await db.commit()
    await db.refresh(user)
    return user


async def get_api_key_user(request: Request, db: AsyncSession) -> dict | None:
    """Valida X-API-Key header. Retorna user data o None."""
    api_key = request.headers.get("X-API-Key", "")
    if not api_key:
        return None

    from app.db.models import ApiKey, User
    result = await db.execute(
        select(ApiKey, User).join(User, ApiKey.user_id == User.id).where(
            ApiKey.key == api_key,
            ApiKey.is_active == True,
            User.is_active == True,
        )
    )
    row = result.first()
    if not row:
        return None

    ak, user = row
    return {"user_id": user.id, "email": user.email, "role": user.role, "plan": user.plan}


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
