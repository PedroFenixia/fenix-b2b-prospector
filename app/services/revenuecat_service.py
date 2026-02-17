"""RevenueCat integration for subscription management.

RevenueCat sits on top of Stripe, managing subscription lifecycle.
Flow:
  1. User clicks "Subscribe" → we create Stripe Checkout via RevenueCat
  2. Stripe processes payment → notifies RevenueCat
  3. RevenueCat webhook notifies our app → we update user plan
  4. We check entitlements via RevenueCat API
"""
from __future__ import annotations

import logging
from typing import Any

import httpx
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings

logger = logging.getLogger(__name__)

RC_BASE = "https://api.revenuecat.com/v1"


def _headers() -> dict[str, str]:
    return {
        "Authorization": f"Bearer {settings.revenuecat_api_key}",
        "Content-Type": "application/json",
    }


async def get_or_create_customer(user_id: int, email: str) -> dict[str, Any]:
    """Get or create a RevenueCat customer (subscriber) for this user."""
    app_user_id = f"fenix_{user_id}"
    async with httpx.AsyncClient(timeout=15) as client:
        # Try to get existing subscriber
        r = await client.get(f"{RC_BASE}/subscribers/{app_user_id}", headers=_headers())
        if r.status_code == 200:
            return r.json()["subscriber"]

        # Create by setting attributes (RevenueCat auto-creates on first call)
        r = await client.post(
            f"{RC_BASE}/subscribers/{app_user_id}/attributes",
            headers=_headers(),
            json={
                "attributes": {
                    "$email": {"value": email},
                    "$displayName": {"value": f"user_{user_id}"},
                }
            },
        )
        r.raise_for_status()

        # Now fetch the created subscriber
        r = await client.get(f"{RC_BASE}/subscribers/{app_user_id}", headers=_headers())
        r.raise_for_status()
        return r.json()["subscriber"]


async def get_entitlements(user_id: int) -> dict[str, Any]:
    """Get active entitlements for a user from RevenueCat."""
    app_user_id = f"fenix_{user_id}"
    async with httpx.AsyncClient(timeout=10) as client:
        r = await client.get(f"{RC_BASE}/subscribers/{app_user_id}", headers=_headers())
        if r.status_code == 404:
            return {}
        r.raise_for_status()
        return r.json()["subscriber"].get("entitlements", {})


def determine_plan_from_entitlements(entitlements: dict) -> str:
    """Determine the user's plan based on active RevenueCat entitlements."""
    active = {k for k, v in entitlements.items() if v.get("expires_date") is None or v.get("expires_date", "") > ""}
    if settings.revenuecat_entitlement_enterprise in active:
        return "enterprise"
    if settings.revenuecat_entitlement_pro in active:
        return "pro"
    return "free"


async def create_stripe_checkout(user_id: int, email: str, plan: str) -> str:
    """Create a Stripe Checkout session via our existing Stripe integration,
    using RevenueCat's app_user_id for tracking.

    RevenueCat will automatically detect the Stripe subscription via webhooks
    once configured in the RevenueCat dashboard (Stripe connection).
    """
    import stripe
    stripe.api_key = settings.stripe_secret_key

    from app.db.engine import async_session
    from app.db.models import User

    async with async_session() as db:
        user = await db.get(User, user_id)

        # Get or create Stripe customer
        if user.stripe_customer_id:
            customer_id = user.stripe_customer_id
        else:
            customer = stripe.Customer.create(
                email=email,
                metadata={
                    "user_id": str(user_id),
                    "rc_app_user_id": f"fenix_{user_id}",
                },
            )
            customer_id = customer.id
            user.stripe_customer_id = customer_id
            await db.commit()

    # Get price ID
    price_id = settings.stripe_price_pro if plan == "pro" else settings.stripe_price_enterprise
    if not price_id:
        raise ValueError(f"No Stripe price configured for plan: {plan}")

    session = stripe.checkout.Session.create(
        customer=customer_id,
        mode="subscription",
        line_items=[{"price": price_id, "quantity": 1}],
        success_url=f"{settings.app_url}/account?upgraded=1",
        cancel_url=f"{settings.app_url}/pricing",
        metadata={
            "user_id": str(user_id),
            "plan": plan,
            "rc_app_user_id": f"fenix_{user_id}",
        },
        subscription_data={
            "metadata": {
                "rc_app_user_id": f"fenix_{user_id}",
            }
        },
    )
    return session.url


async def handle_revenuecat_webhook(event: dict, db: AsyncSession) -> str:
    """Process RevenueCat webhook events to update user plans.

    RevenueCat sends events like:
    - INITIAL_PURCHASE: New subscription
    - RENEWAL: Subscription renewed
    - CANCELLATION: Subscription cancelled
    - EXPIRATION: Subscription expired
    - BILLING_ISSUE: Payment failed
    """
    from app.db.models import User

    event_type = event.get("type", "UNKNOWN")
    app_user_id = event.get("app_user_id", "")

    logger.info("RevenueCat webhook: %s for %s", event_type, app_user_id)

    # Extract our user_id from app_user_id (format: "fenix_{user_id}")
    if not app_user_id.startswith("fenix_"):
        logger.warning("Unknown app_user_id format: %s", app_user_id)
        return event_type

    try:
        user_id = int(app_user_id.replace("fenix_", ""))
    except ValueError:
        logger.warning("Invalid user_id in app_user_id: %s", app_user_id)
        return event_type

    user = await db.get(User, user_id)
    if not user:
        logger.warning("User %d not found for webhook", user_id)
        return event_type

    if event_type in ("INITIAL_PURCHASE", "RENEWAL", "UNCANCELLATION"):
        # Determine plan from entitlements in the event
        entitlements = event.get("subscriber", {}).get("entitlements", {})
        new_plan = determine_plan_from_entitlements(entitlements)
        if new_plan != "free":
            user.plan = new_plan
            logger.info("User %d upgraded to %s via RevenueCat", user_id, new_plan)
        await db.commit()

    elif event_type in ("CANCELLATION", "EXPIRATION", "BILLING_ISSUE"):
        user.plan = "free"
        user.stripe_subscription_id = None
        logger.info("User %d downgraded to free (%s)", user_id, event_type)
        await db.commit()

    return event_type
