"""Servicio de vigilancia y alertas."""
from __future__ import annotations

import logging
from datetime import date
from typing import Optional

from sqlalchemy import delete, func, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import joinedload

from app.db.models import Act, ActTypeWatch, Alert, Company, Watchlist

logger = logging.getLogger(__name__)


async def add_to_watchlist(
    company_id: int,
    notas: str | None,
    db: AsyncSession,
    tipos_acto: list[str] | None = None,
    user_id: int | None = None,
) -> Watchlist | None:
    """Añadir empresa a la watchlist con filtro opcional de tipos de acto."""
    import json
    tipos_json = json.dumps(tipos_acto) if tipos_acto else None
    query = select(Watchlist).where(Watchlist.company_id == company_id)
    if user_id:
        query = query.where(Watchlist.user_id == user_id)
    existing = await db.scalar(query)
    if existing:
        existing.notas = notas
        existing.tipos_acto = tipos_json
        await db.commit()
        return existing
    entry = Watchlist(company_id=company_id, notas=notas, tipos_acto=tipos_json, user_id=user_id)
    db.add(entry)
    await db.commit()
    await db.refresh(entry)
    return entry


async def remove_from_watchlist(company_id: int, db: AsyncSession, user_id: int | None = None) -> bool:
    """Eliminar empresa de la watchlist."""
    query = delete(Watchlist).where(Watchlist.company_id == company_id)
    if user_id:
        query = query.where(Watchlist.user_id == user_id)
    result = await db.execute(query)
    await db.commit()
    return result.rowcount > 0


async def is_watched(company_id: int, db: AsyncSession, user_id: int | None = None) -> bool:
    """Comprobar si una empresa esta en la watchlist."""
    query = select(Watchlist.id).where(Watchlist.company_id == company_id)
    if user_id:
        query = query.where(Watchlist.user_id == user_id)
    result = await db.scalar(query)
    return result is not None


async def get_watchlist(db: AsyncSession, page: int = 1, per_page: int = 25, user_id: int | None = None) -> dict:
    """Obtener listado de empresas vigiladas."""
    count_q = select(func.count(Watchlist.id))
    items_q = select(Watchlist).options(joinedload(Watchlist.company))
    if user_id:
        count_q = count_q.where(Watchlist.user_id == user_id)
        items_q = items_q.where(Watchlist.user_id == user_id)

    total = await db.scalar(count_q)
    offset = (page - 1) * per_page
    items = await db.scalars(
        items_q.order_by(Watchlist.created_at.desc()).offset(offset).limit(per_page)
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
    user_id: int | None = None,
    source: str | None = None,
) -> dict:
    """Obtener alertas."""
    count_q = select(func.count(Alert.id))
    items_q = select(Alert).options(joinedload(Alert.company), joinedload(Alert.act))

    if user_id:
        count_q = count_q.where(Alert.user_id == user_id)
        items_q = items_q.where(Alert.user_id == user_id)
    if solo_no_leidas:
        count_q = count_q.where(Alert.leida == False)
        items_q = items_q.where(Alert.leida == False)
    if source:
        count_q = count_q.where(Alert.source == source)
        items_q = items_q.where(Alert.source == source)

    total = await db.scalar(count_q)
    offset = (page - 1) * per_page
    items = await db.scalars(
        items_q.order_by(Alert.created_at.desc()).offset(offset).limit(per_page)
    )

    return {
        "items": items.unique().all(),
        "total": total or 0,
        "page": page,
        "pages": max(1, -(-total // per_page)) if total else 1,
    }


async def count_unread_alerts(db: AsyncSession, user_id: int | None = None) -> int:
    """Contar alertas sin leer."""
    query = select(func.count(Alert.id)).where(Alert.leida == False)
    if user_id:
        query = query.where(Alert.user_id == user_id)
    return await db.scalar(query) or 0


async def mark_alert_read(alert_id: int, db: AsyncSession) -> bool:
    """Marcar alerta como leida."""
    alert = await db.get(Alert, alert_id)
    if not alert:
        return False
    alert.leida = True
    await db.commit()
    return True


async def mark_all_read(db: AsyncSession, user_id: int | None = None) -> int:
    """Marcar todas las alertas como leidas."""
    from sqlalchemy import update
    query = update(Alert).where(Alert.leida == False).values(leida=True)
    if user_id:
        query = query.where(Alert.user_id == user_id)
    result = await db.execute(query)
    await db.commit()
    return result.rowcount


async def add_act_type_watch(
    user_id: int,
    tipo_acto: str,
    db: AsyncSession,
    filtro_provincia: str | None = None,
) -> ActTypeWatch | None:
    """Crear suscripción global a un tipo de acto."""
    prov = filtro_provincia or None
    query = select(ActTypeWatch).where(
        ActTypeWatch.user_id == user_id,
        ActTypeWatch.tipo_acto == tipo_acto,
    )
    if prov:
        query = query.where(ActTypeWatch.filtro_provincia == prov)
    else:
        query = query.where(ActTypeWatch.filtro_provincia.is_(None))
    existing = await db.scalar(query)
    if existing:
        existing.is_active = True
        await db.commit()
        return existing
    entry = ActTypeWatch(
        user_id=user_id,
        tipo_acto=tipo_acto,
        filtro_provincia=prov,
    )
    db.add(entry)
    await db.commit()
    await db.refresh(entry)
    return entry


async def remove_act_type_watch(watch_id: int, user_id: int, db: AsyncSession) -> bool:
    """Eliminar suscripción global a un tipo de acto."""
    result = await db.execute(
        delete(ActTypeWatch).where(
            ActTypeWatch.id == watch_id,
            ActTypeWatch.user_id == user_id,
        )
    )
    await db.commit()
    return result.rowcount > 0


async def get_act_type_watches(user_id: int, db: AsyncSession) -> list[ActTypeWatch]:
    """Obtener suscripciones activas de tipos de acto para un usuario."""
    result = await db.scalars(
        select(ActTypeWatch)
        .where(ActTypeWatch.user_id == user_id, ActTypeWatch.is_active == True)
        .order_by(ActTypeWatch.created_at.desc())
    )
    return result.all()


async def generate_alerts_for_date(fecha: date, db: AsyncSession) -> int:
    """Generar alertas para empresas vigiladas y suscripciones globales por tipo de acto.

    Paso 1: Alertas de watchlist (empresas concretas).
    Paso 2: Alertas de ActTypeWatch (tipos de acto globales).
    Evita duplicados entre ambos pasos.
    """
    import json as _json

    count = 0
    alerted_keys: set[tuple[int | None, int, int]] = set()  # (user_id, company_id, act_id)

    # --- Paso 1: Alertas de watchlist (empresas concretas) ---
    watchlist_entries = (await db.scalars(select(Watchlist))).all()

    if watchlist_entries:
        watch_map: list[tuple[int, int | None, set[str] | None]] = []
        company_ids = set()
        for entry in watchlist_entries:
            tipos = set(_json.loads(entry.tipos_acto)) if entry.tipos_acto else None
            watch_map.append((entry.company_id, entry.user_id, tipos))
            company_ids.add(entry.company_id)

        acts = await db.scalars(
            select(Act)
            .options(joinedload(Act.company))
            .where(
                Act.fecha_publicacion == fecha,
                Act.company_id.in_(company_ids),
            )
        )

        for act in acts.unique().all():
            for cid, uid, allowed in watch_map:
                if cid != act.company_id:
                    continue
                if allowed is not None and act.tipo_acto not in allowed:
                    continue

                key = (uid, act.company_id, act.id)
                if key in alerted_keys:
                    continue
                alerted_keys.add(key)

                alert = Alert(
                    user_id=uid,
                    company_id=act.company_id,
                    act_id=act.id,
                    tipo=act.tipo_acto,
                    titulo=f"{act.company.nombre}: {act.tipo_acto}",
                    descripcion=act.texto_original[:500] if act.texto_original else None,
                    source="watchlist",
                )
                db.add(alert)
                count += 1

    # --- Paso 2: Alertas de ActTypeWatch (tipos de acto globales) ---
    type_watches = (
        await db.scalars(select(ActTypeWatch).where(ActTypeWatch.is_active == True))
    ).all()

    if type_watches:
        # Group by tipo_acto for efficient querying
        tipos_needed = {tw.tipo_acto for tw in type_watches}
        global_acts = await db.scalars(
            select(Act)
            .options(joinedload(Act.company))
            .where(
                Act.fecha_publicacion == fecha,
                Act.tipo_acto.in_(tipos_needed),
            )
        )

        for act in global_acts.unique().all():
            for tw in type_watches:
                if tw.tipo_acto != act.tipo_acto:
                    continue
                if tw.filtro_provincia and act.company.provincia != tw.filtro_provincia:
                    continue

                key = (tw.user_id, act.company_id, act.id)
                if key in alerted_keys:
                    continue
                alerted_keys.add(key)

                provincia_str = f" ({act.company.provincia})" if act.company.provincia else ""
                alert = Alert(
                    user_id=tw.user_id,
                    company_id=act.company_id,
                    act_id=act.id,
                    tipo=act.tipo_acto,
                    titulo=f"[{act.tipo_acto}] {act.company.nombre}{provincia_str}",
                    descripcion=act.texto_original[:500] if act.texto_original else None,
                    source="act_type",
                )
                db.add(alert)
                count += 1

    if count > 0:
        await db.commit()
        logger.info("Generadas %d alertas para fecha %s", count, fecha)

    return count
