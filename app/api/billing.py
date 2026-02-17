"""Billing API endpoints for Stripe + RevenueCat integration."""
from __future__ import annotations

import logging

from fastapi import APIRouter, Depends, Request
from fastapi.responses import JSONResponse, RedirectResponse
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.db.engine import get_db

logger = logging.getLogger(__name__)

router = APIRouter()


@router.post("/checkout")
async def create_checkout(request: Request, db: AsyncSession = Depends(get_db)):
    """Create a Stripe Checkout session for upgrading (tracked by RevenueCat)."""
    user = getattr(request.state, "user", None)
    if not user:
        return JSONResponse({"error": "No autenticado"}, status_code=401)

    body = await request.json()
    plan = body.get("plan", "pro")
    if plan not in ("pro", "enterprise"):
        return JSONResponse({"error": "Plan invalido"}, status_code=400)

    try:
        # Use RevenueCat-aware checkout if RC is configured, else direct Stripe
        if settings.revenuecat_api_key:
            from app.services.revenuecat_service import create_stripe_checkout
            url = await create_stripe_checkout(user["user_id"], user["email"], plan)
        else:
            from app.services.stripe_service import create_checkout_session
            url = await create_checkout_session(user["user_id"], user["email"], plan, db)
        return {"url": url}
    except ValueError as e:
        return JSONResponse({"error": str(e)}, status_code=400)
    except Exception as e:
        logger.exception("Checkout error")
        return JSONResponse({"error": f"Error al crear sesion de pago: {e}"}, status_code=500)


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
    """Handle Stripe webhook events (fallback, RevenueCat is primary)."""
    payload = await request.body()
    sig_header = request.headers.get("stripe-signature", "")

    from app.services.stripe_service import handle_webhook
    try:
        event_type = await handle_webhook(payload, sig_header, db)
        return {"received": True, "type": event_type}
    except Exception as e:
        logger.exception("Stripe webhook error")
        return JSONResponse({"error": str(e)}, status_code=400)


@router.post("/rc-webhook")
async def revenuecat_webhook(request: Request, db: AsyncSession = Depends(get_db)):
    """Handle RevenueCat webhook events (primary subscription lifecycle)."""
    # Verify authorization
    auth_header = request.headers.get("authorization", "")
    if settings.revenuecat_webhook_auth and auth_header != settings.revenuecat_webhook_auth:
        return JSONResponse({"error": "Unauthorized"}, status_code=401)

    body = await request.json()
    event = body.get("event", body)

    from app.services.revenuecat_service import handle_revenuecat_webhook
    try:
        event_type = await handle_revenuecat_webhook(event, db)
        return {"received": True, "type": event_type}
    except Exception as e:
        logger.exception("RevenueCat webhook error")
        return JSONResponse({"error": str(e)}, status_code=400)
