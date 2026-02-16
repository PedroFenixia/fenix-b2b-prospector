"""Stripe integration for subscription management."""
from __future__ import annotations

import logging

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings

logger = logging.getLogger(__name__)


def _get_stripe():
    """Lazy import stripe to avoid crash when not installed."""
    import stripe
    stripe.api_key = settings.stripe_secret_key
    return stripe


async def create_checkout_session(
    user_id: int,
    email: str,
    plan: str,
    db: AsyncSession,
) -> str:
    """Create a Stripe Checkout session and return the URL."""
    stripe = _get_stripe()
    from app.db.models import User

    user = await db.get(User, user_id)

    # Get or create Stripe customer
    if user.stripe_customer_id:
        customer_id = user.stripe_customer_id
    else:
        customer = stripe.Customer.create(email=email, metadata={"user_id": str(user_id)})
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
        metadata={"user_id": str(user_id), "plan": plan},
    )
    return session.url


async def create_portal_session(user_id: int, db: AsyncSession) -> str:
    """Create a Stripe Customer Portal session for managing subscription."""
    stripe = _get_stripe()
    from app.db.models import User

    user = await db.get(User, user_id)
    if not user.stripe_customer_id:
        raise ValueError("User has no Stripe customer ID")

    session = stripe.billing_portal.Session.create(
        customer=user.stripe_customer_id,
        return_url=f"{settings.app_url}/account",
    )
    return session.url


async def handle_webhook(payload: bytes, sig_header: str, db: AsyncSession) -> str:
    """Process Stripe webhook events. Returns event type handled."""
    stripe = _get_stripe()
    from app.db.models import User

    event = stripe.Webhook.construct_event(
        payload, sig_header, settings.stripe_webhook_secret,
    )

    event_type = event["type"]
    logger.info("Stripe webhook: %s", event_type)

    if event_type == "checkout.session.completed":
        session = event["data"]["object"]
        user_id = int(session["metadata"]["user_id"])
        plan = session["metadata"]["plan"]
        subscription_id = session.get("subscription")

        user = await db.get(User, user_id)
        if user:
            user.plan = plan
            user.stripe_subscription_id = subscription_id
            if not user.stripe_customer_id:
                user.stripe_customer_id = session.get("customer")
            await db.commit()
            logger.info("User %d upgraded to %s", user_id, plan)

    elif event_type in ("customer.subscription.updated", "customer.subscription.deleted"):
        subscription = event["data"]["object"]
        customer_id = subscription["customer"]

        user = await db.scalar(
            select(User).where(User.stripe_customer_id == customer_id)
        )
        if user:
            status = subscription["status"]
            if event_type == "customer.subscription.deleted" or status in ("canceled", "unpaid", "past_due"):
                user.plan = "free"
                user.stripe_subscription_id = None
                logger.info("User %d downgraded to free (status: %s)", user.id, status)
            elif status == "active":
                # Check which price to determine plan
                items = subscription.get("items", {}).get("data", [])
                if items:
                    price_id = items[0]["price"]["id"]
                    if price_id == settings.stripe_price_enterprise:
                        user.plan = "enterprise"
                    elif price_id == settings.stripe_price_pro:
                        user.plan = "pro"
            await db.commit()

    return event_type
