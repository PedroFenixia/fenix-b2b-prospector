"""Billing API endpoints for Stripe integration."""
from __future__ import annotations

from fastapi import APIRouter, Depends, Request
from fastapi.responses import JSONResponse, RedirectResponse
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.engine import get_db

router = APIRouter()


@router.post("/checkout")
async def create_checkout(request: Request, db: AsyncSession = Depends(get_db)):
    """Create a Stripe Checkout session for upgrading."""
    user = getattr(request.state, "user", None)
    if not user:
        return JSONResponse({"error": "No autenticado"}, status_code=401)

    body = await request.json()
    plan = body.get("plan", "pro")
    if plan not in ("pro", "enterprise"):
        return JSONResponse({"error": "Plan invalido"}, status_code=400)

    from app.services.stripe_service import create_checkout_session
    try:
        url = await create_checkout_session(user["user_id"], user["email"], plan, db)
        return {"url": url}
    except ValueError as e:
        return JSONResponse({"error": str(e)}, status_code=400)
    except Exception as e:
        return JSONResponse({"error": f"Error de Stripe: {e}"}, status_code=500)


@router.get("/portal")
async def billing_portal(request: Request, db: AsyncSession = Depends(get_db)):
    """Redirect to Stripe Customer Portal for subscription management."""
    user = getattr(request.state, "user", None)
    if not user:
        return RedirectResponse(url="/login", status_code=302)

    from app.services.stripe_service import create_portal_session
    try:
        url = await create_portal_session(user["user_id"], db)
        return RedirectResponse(url=url, status_code=302)
    except ValueError:
        return RedirectResponse(url="/pricing", status_code=302)


@router.post("/webhook")
async def stripe_webhook(request: Request, db: AsyncSession = Depends(get_db)):
    """Handle Stripe webhook events."""
    payload = await request.body()
    sig_header = request.headers.get("stripe-signature", "")

    from app.services.stripe_service import handle_webhook
    try:
        event_type = await handle_webhook(payload, sig_header, db)
        return {"received": True, "type": event_type}
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=400)
