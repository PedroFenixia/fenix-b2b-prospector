"""Servicio de vigilancia y alertas."""
from __future__ import annotations

import logging
from datetime import date
from typing import Optional

from sqlalchemy import delete, func, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import joinedload

from app.db.models import Act, Alert, Company, Watchlist

logger = logging.getLogger(__name__)


async def add_to_watchlist(
    company_id: int,
    notas: str | None,
    db: AsyncSession,
    tipos_acto: list[str] | None = None,
) -> Watchlist | None:
    """Añadir empresa a la watchlist con filtro opcional de tipos de acto."""
    import json
    tipos_json = json.dumps(tipos_acto) if tipos_acto else None
    existing = await db.scalar(select(Watchlist).where(Watchlist.company_id == company_id))
    if existing:
        existing.notas = notas
        existing.tipos_acto = tipos_json
        await db.commit()
        return existing
    entry = Watchlist(company_id=company_id, notas=notas, tipos_acto=tipos_json)
    db.add(entry)
    await db.commit()
    await db.refresh(entry)
    return entry


async def remove_from_watchlist(company_id: int, db: AsyncSession) -> bool:
    """Eliminar empresa de la watchlist."""
    result = await db.execute(delete(Watchlist).where(Watchlist.company_id == company_id))
    await db.commit()
    return result.rowcount > 0


async def is_watched(company_id: int, db: AsyncSession) -> bool:
    """Comprobar si una empresa esta en la watchlist."""
    result = await db.scalar(select(Watchlist.id).where(Watchlist.company_id == company_id))
    return result is not None


async def get_watchlist(db: AsyncSession, page: int = 1, per_page: int = 25) -> dict:
    """Obtener listado de empresas vigiladas."""
    total = await db.scalar(select(func.count(Watchlist.id)))
    offset = (page - 1) * per_page
    items = await db.scalars(
        select(Watchlist)
        .options(joinedload(Watchlist.company))
        .order_by(Watchlist.created_at.desc())
        .offset(offset)
        .limit(per_page)
    )
    return {
        "items": items.unique().all(),
        "total": total or 0,
        "page": page,
        "pages": max(1, -(-total // per_page)) if total else 1,
    }


async def get_alerts(
    db: AsyncSession,
    solo_no_leidas: bool = False,
    page: int = 1,
    per_page: int = 25,
) -> dict:
    """Obtener alertas."""
    query = select(func.count(Alert.id))
    if solo_no_leidas:
        query = query.where(Alert.leida == False)
    total = await db.scalar(query)

    offset = (page - 1) * per_page
    items_q = (
        select(Alert)
        .options(joinedload(Alert.company), joinedload(Alert.act))
        .order_by(Alert.created_at.desc())
        .offset(offset)
        .limit(per_page)
    )
    if solo_no_leidas:
        items_q = items_q.where(Alert.leida == False)
    items = await db.scalars(items_q)

    return {
        "items": items.unique().all(),
        "total": total or 0,
        "page": page,
        "pages": max(1, -(-total // per_page)) if total else 1,
    }


async def count_unread_alerts(db: AsyncSession) -> int:
    """Contar alertas sin leer."""
    return await db.scalar(select(func.count(Alert.id)).where(Alert.leida == False)) or 0


async def mark_alert_read(alert_id: int, db: AsyncSession) -> bool:
    """Marcar alerta como leida."""
    alert = await db.get(Alert, alert_id)
    if not alert:
        return False
    alert.leida = True
    await db.commit()
    return True


async def mark_all_read(db: AsyncSession) -> int:
    """Marcar todas las alertas como leidas."""
    from sqlalchemy import update
    result = await db.execute(update(Alert).where(Alert.leida == False).values(leida=True))
    await db.commit()
    return result.rowcount


async def generate_alerts_for_date(fecha: date, db: AsyncSession) -> int:
    """Generar alertas para empresas vigiladas que tuvieron actividad en una fecha.

    Respeta el filtro tipos_acto de cada watchlist entry (null = todos los tipos).
    """
    import json as _json

    # Obtener watchlist con filtros
    watchlist_entries = (await db.scalars(select(Watchlist))).all()
    if not watchlist_entries:
        return 0

    # Build map: company_id -> allowed act types (None = all)
    watch_filters: dict[int, set[str] | None] = {}
    for entry in watchlist_entries:
        if entry.tipos_acto:
            watch_filters[entry.company_id] = set(_json.loads(entry.tipos_acto))
        else:
            watch_filters[entry.company_id] = None  # all types

    # Buscar actos de esa fecha para empresas vigiladas
    acts = await db.scalars(
        select(Act)
        .options(joinedload(Act.company))
        .where(
            Act.fecha_publicacion == fecha,
            Act.company_id.in_(watch_filters.keys()),
        )
    )

    count = 0
    for act in acts.unique().all():
        allowed = watch_filters.get(act.company_id)
        # Skip if act type not in the filter
        if allowed is not None and act.tipo_acto not in allowed:
            continue

        alert = Alert(
            company_id=act.company_id,
            act_id=act.id,
            tipo=act.tipo_acto,
            titulo=f"{act.company.nombre}: {act.tipo_acto}",
            descripcion=act.texto_original[:500] if act.texto_original else None,
        )
        db.add(alert)
        count += 1

    if count > 0:
        await db.commit()
        logger.info("Generadas %d alertas para fecha %s", count, fecha)

    return count
