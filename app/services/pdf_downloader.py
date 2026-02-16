from __future__ import annotations

"""Download BORME PDF files."""
import asyncio
import logging
from pathlib import Path

import httpx

from app.config import settings
from app.services.borme_fetcher import BormePdfEntry

logger = logging.getLogger(__name__)


async def download_pdfs(
    pdfs: list[BormePdfEntry],
    fecha_str: str,
) -> list[tuple[BormePdfEntry, Path]]:
    """
    Download PDF files to data/borme_pdfs/{YYYY}/{MM}/{filename}.pdf.
    Skips already-downloaded files (idempotent).
    Returns list of (entry, local_path) tuples for successfully downloaded files.
    """
    results: list[tuple[BormePdfEntry, Path]] = []
    semaphore = asyncio.Semaphore(settings.pdf_download_concurrency)

    year, month = fecha_str[:4], fecha_str[4:6]
    out_dir = settings.borme_pdf_dir / year / month
    out_dir.mkdir(parents=True, exist_ok=True)

    async with httpx.AsyncClient(timeout=60.0, follow_redirects=True) as client:

        async def download_one(entry: BormePdfEntry) -> tuple[BormePdfEntry, Path] | None:
            filename = entry.id.replace("/", "_") + ".pdf"
            local_path = out_dir / filename

            if local_path.exists() and local_path.stat().st_size > 0:
                logger.debug(f"Already downloaded: {filename}")
                return (entry, local_path)

            async with semaphore:
                try:
                    resp = await client.get(entry.url_pdf)
                    if resp.status_code == 200:
                        local_path.write_bytes(resp.content)
                        logger.info(f"Downloaded: {filename} ({len(resp.content)} bytes)")
                        return (entry, local_path)
                    else:
                        logger.warning(f"Failed to download {entry.url_pdf}: HTTP {resp.status_code}")
                        return None
                except Exception as e:
                    logger.error(f"Error downloading {entry.url_pdf}: {e}")
                    return None

        tasks = [download_one(entry) for entry in pdfs]
        completed = await asyncio.gather(*tasks)

    for result in completed:
        if result is not None:
            results.append(result)

    logger.info(f"Downloaded {len(results)}/{len(pdfs)} PDFs")
    return results
