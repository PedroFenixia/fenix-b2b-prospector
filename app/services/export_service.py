"""Export companies to CSV/Excel."""
import csv
import io
import logging
from datetime import datetime
from pathlib import Path

from openpyxl import Workbook
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.db.models import ExportLog
from app.schemas.search import SearchFilters
from app.services.company_service import search_companies

logger = logging.getLogger(__name__)

EXPORT_FIELDS = [
    ("nombre", "Nombre"),
    ("cif", "CIF"),
    ("forma_juridica", "Forma Jurídica"),
    ("domicilio", "Domicilio"),
    ("provincia", "Provincia"),
    ("localidad", "Localidad"),
    ("objeto_social", "Objeto Social"),
    ("cnae_code", "CNAE"),
    ("capital_social", "Capital Social (€)"),
    ("fecha_constitucion", "Fecha Constitución"),
    ("fecha_primera_publicacion", "Primera Publicación BORME"),
    ("fecha_ultima_publicacion", "Última Publicación BORME"),
    ("estado", "Estado"),
]


async def _increment_export_count(user_id: int | None, db: AsyncSession):
    """Increment user's monthly export counter."""
    if not user_id:
        return
    from app.db.models import User
    from datetime import datetime as _dt
    user = await db.get(User, user_id)
    if not user:
        return
    current_month = _dt.now().strftime("%Y-%m")
    if user.month_reset != current_month:
        user.searches_this_month = 0
        user.exports_this_month = 0
        user.month_reset = current_month
    user.exports_this_month += 1
    await db.commit()


async def export_csv(filters: SearchFilters, db: AsyncSession, user_id: int | None = None) -> Path:
    """Export search results as CSV."""
    # Get all results (override pagination)
    filters.per_page = 100
    filters.page = 1
    all_items = []

    while True:
        result = await search_companies(filters, db)
        all_items.extend(result["items"])
        if filters.page >= result["pages"]:
            break
        filters.page += 1

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"export_{timestamp}.csv"
    filepath = settings.export_dir / filename
    settings.export_dir.mkdir(parents=True, exist_ok=True)

    with open(filepath, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.writer(f, delimiter=";")
        writer.writerow([label for _, label in EXPORT_FIELDS])

        for company in all_items:
            row = []
            for field_name, _ in EXPORT_FIELDS:
                value = getattr(company, field_name, "")
                row.append(str(value) if value is not None else "")
            writer.writerow(row)

    # Log export
    log = ExportLog(
        user_id=user_id,
        filename=filename,
        format="csv",
        filters_applied=filters.model_dump_json(),
        record_count=len(all_items),
    )
    db.add(log)
    await db.commit()

    await _increment_export_count(user_id, db)

    logger.info(f"Exported {len(all_items)} companies to {filename}")
    return filepath


async def export_excel(filters: SearchFilters, db: AsyncSession, user_id: int | None = None) -> Path:
    """Export search results as Excel."""
    filters.per_page = 100
    filters.page = 1
    all_items = []

    while True:
        result = await search_companies(filters, db)
        all_items.extend(result["items"])
        if filters.page >= result["pages"]:
            break
        filters.page += 1

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"export_{timestamp}.xlsx"
    filepath = settings.export_dir / filename
    settings.export_dir.mkdir(parents=True, exist_ok=True)

    wb = Workbook()
    ws = wb.active
    ws.title = "Empresas"

    # Header row
    headers = [label for _, label in EXPORT_FIELDS]
    ws.append(headers)

    # Style header
    from openpyxl.styles import Font
    for cell in ws[1]:
        cell.font = Font(bold=True)

    # Data rows
    for company in all_items:
        row = []
        for field_name, _ in EXPORT_FIELDS:
            value = getattr(company, field_name, "")
            if value is None:
                value = ""
            row.append(value)
        ws.append(row)

    # Auto-width columns
    for col in ws.columns:
        max_length = max(len(str(cell.value or "")) for cell in col)
        ws.column_dimensions[col[0].column_letter].width = min(max_length + 2, 50)

    wb.save(filepath)

    log = ExportLog(
        user_id=user_id,
        filename=filename,
        format="xlsx",
        filters_applied=filters.model_dump_json(),
        record_count=len(all_items),
    )
    db.add(log)
    await db.commit()

    await _increment_export_count(user_id, db)

    logger.info(f"Exported {len(all_items)} companies to {filename}")
    return filepath
