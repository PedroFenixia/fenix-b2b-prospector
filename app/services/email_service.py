"""Email service for sending verification codes via SMTP."""
from __future__ import annotations

import asyncio
import logging
import random
import smtplib
import string
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

from app.config import settings

logger = logging.getLogger(__name__)


def generate_code(length: int = 6) -> str:
    """Generate a random numeric verification code."""
    return "".join(random.choices(string.digits, k=length))


def _send_smtp(to_email: str, subject: str, html_body: str) -> bool:
    """Send email via SMTP (blocking). Run in executor."""
    if not settings.smtp_host:
        logger.warning("SMTP not configured, skipping email to %s", to_email)
        return False

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = f"{settings.smtp_from_name} <{settings.smtp_from}>"
    msg["To"] = to_email
    msg.attach(MIMEText(html_body, "html"))

    try:
        with smtplib.SMTP(settings.smtp_host, settings.smtp_port, timeout=10) as server:
            server.starttls()
            server.login(settings.smtp_user, settings.smtp_password)
            server.sendmail(settings.smtp_from, to_email, msg.as_string())
        logger.info("Email sent to %s", to_email)
        return True
    except Exception:
        logger.exception("Failed to send email to %s", to_email)
        return False


async def send_email_async(to_email: str, subject: str, html_body: str) -> bool:
    """Send email without blocking the event loop."""
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(None, _send_smtp, to_email, subject, html_body)


async def send_verification_email(to_email: str, code: str, nombre: str) -> bool:
    """Send verification code email."""
    html = f"""
    <div style="font-family: 'Inter', Arial, sans-serif; max-width: 480px; margin: 0 auto; padding: 32px;">
        <div style="text-align: center; margin-bottom: 32px;">
            <h1 style="color: #dc2626; font-size: 24px; font-weight: 800; margin: 0;">FENIX B2B Prospector</h1>
        </div>
        <div style="background: #fff; border: 1px solid #e5e7eb; border-radius: 16px; padding: 32px;">
            <h2 style="color: #111827; font-size: 20px; margin: 0 0 8px;">Verifica tu email</h2>
            <p style="color: #6b7280; font-size: 14px; margin: 0 0 24px;">
                Hola {nombre}, usa este codigo para verificar tu cuenta:
            </p>
            <div style="background: #f9fafb; border-radius: 12px; padding: 20px; text-align: center; margin-bottom: 24px;">
                <span style="font-family: 'JetBrains Mono', monospace; font-size: 36px; font-weight: 700; letter-spacing: 8px; color: #111827;">{code}</span>
            </div>
            <p style="color: #9ca3af; font-size: 12px; margin: 0;">
                Este codigo expira en 30 minutos. Si no has solicitado esta verificacion, ignora este email.
            </p>
        </div>
        <p style="text-align: center; color: #d1d5db; font-size: 11px; margin-top: 24px;">
            &copy; 2026 FENIX IA SOLUTIONS
        </p>
    </div>
    """
    return await send_email_async(to_email, "Tu codigo de verificacion - FENIX B2B", html)
