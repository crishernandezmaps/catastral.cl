"""Resend email client — ported from prediosChile email.js."""
import logging
import httpx
from config import RESEND_API_KEY

logger = logging.getLogger("email")

RESEND_URL = "https://api.resend.com/emails"
FROM_EMAIL = "Catastro Chile <no-reply@tremen.tech>"


async def _send(to: str, subject: str, html: str):
    if not RESEND_API_KEY:
        logger.warning(f"RESEND_API_KEY not set, skipping email to {to}")
        return
    async with httpx.AsyncClient() as client:
        resp = await client.post(
            RESEND_URL,
            headers={"Authorization": f"Bearer {RESEND_API_KEY}"},
            json={
                "from": FROM_EMAIL,
                "to": [to],
                "subject": subject,
                "html": html,
            },
            timeout=10,
        )
        if not resp.is_success:
            logger.error(f"Resend error: {resp.status_code} {resp.text[:200]}")
            raise Exception(f"Email send failed: {resp.status_code}")


async def send_otp_email(email: str, code: str):
    html = f"""
    <div style="font-family: Inter, sans-serif; max-width: 400px; margin: 0 auto; padding: 32px;">
        <h2 style="color: #bafb00; margin-bottom: 8px;">Catastro Chile</h2>
        <p>Tu codigo de acceso es:</p>
        <div style="background: #1a1a1a; color: #bafb00; font-size: 32px; font-weight: bold;
                    letter-spacing: 8px; text-align: center; padding: 20px; border-radius: 8px;
                    margin: 16px 0;">
            {code}
        </div>
        <p style="color: #888; font-size: 13px;">Este codigo expira en 10 minutos.</p>
    </div>
    """
    await _send(email, f"Tu codigo de acceso: {code}", html)


async def send_grant_notification(email: str, package_name: str, duration: str):
    html = f"""
    <div style="font-family: Inter, sans-serif; max-width: 400px; margin: 0 auto; padding: 32px;">
        <h2 style="color: #bafb00;">Acceso otorgado</h2>
        <p>Se te ha otorgado acceso a:</p>
        <div style="background: #1a1a1a; color: #fff; padding: 16px; border-radius: 8px; margin: 16px 0;">
            <strong>{package_name}</strong><br>
            <span style="color: #888;">Duracion: {duration}</span>
        </div>
        <p>Ingresa a <a href="https://catastral.cl" style="color: #bafb00;">catastral.cl</a> para descargar tus datos.</p>
    </div>
    """
    await _send(email, f"Acceso otorgado: {package_name}", html)
