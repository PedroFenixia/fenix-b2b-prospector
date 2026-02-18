FROM python:3.11-slim AS builder

WORKDIR /app

# System deps for bcrypt (gcc), lxml, and psycopg2
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc libffi-dev libpq-dev && rm -rf /var/lib/apt/lists/*

COPY pyproject.toml .
RUN pip install --no-cache-dir .

# --- Production image ---
FROM python:3.11-slim

WORKDIR /app

# Runtime deps only (libpq for psycopg2)
RUN apt-get update && apt-get install -y --no-install-recommends \
    libpq5 curl && rm -rf /var/lib/apt/lists/*

# Copy installed packages from builder
COPY --from=builder /usr/local/lib/python3.11/site-packages /usr/local/lib/python3.11/site-packages
COPY --from=builder /usr/local/bin /usr/local/bin

# Copy application code
COPY . .

# Create data directories
RUN mkdir -p data/borme_pdfs data/exports

EXPOSE 8000

HEALTHCHECK --interval=30s --timeout=5s --retries=3 \
    CMD curl -f http://localhost:8000/health || exit 1

# Production: no --reload, 2 workers
CMD ["python", "-m", "uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "2"]
