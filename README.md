# FENIX Prospector

Sistema de prospección de leads desde registros públicos españoles (BOE/BORME).

Extrae datos de empresas del Boletín Oficial del Registro Mercantil y los presenta en un dashboard web para prospección comercial.

## Qué hace

- **Ingesta automática** desde la API del BOE/BORME (sumarios diarios)
- **Parsea PDFs** del BORME para extraer datos estructurados de empresas
- **Dashboard web** con búsqueda por filtros, detalle de empresa y panel de ingesta
- **Exporta CSV/Excel** para importar en cualquier CRM

## Datos que extrae

De cada empresa publicada en el BORME:

| Campo | Fuente |
|-------|--------|
| Nombre | BORME |
| Forma jurídica (SL, SA...) | Inferido del nombre |
| Domicilio | Acto de Constitución |
| Provincia | Sección del BORME |
| Capital social | Acto de Constitución |
| Objeto social (actividad) | Acto de Constitución |
| CNAE (sector) | Estimado por keywords |
| Cargos (administradores, etc.) | Nombramientos/Ceses |
| Historial de actos | Todos los actos publicados |

> **Nota:** El BORME no publica el CIF. El campo existe para enriquecimiento manual.

## Stack

- **Backend:** Python 3.9+ / FastAPI (async)
- **Frontend:** Jinja2 + HTMX + Tailwind CSS (CDN)
- **Base de datos:** SQLite + FTS5
- **PDF parsing:** pdfminer.six + regex
- **Sin dependencias de build:** No requiere npm, webpack ni Node.js

## Instalación

```bash
# Clonar
git clone https://github.com/tu-usuario/fenix-prospector.git
cd fenix-prospector

# Entorno virtual
python3 -m venv .venv
source .venv/bin/activate

# Dependencias
pip install --upgrade pip
pip install fastapi "uvicorn[standard]" jinja2 httpx sqlalchemy aiosqlite \
    alembic "pdfminer.six" lxml openpyxl apscheduler pydantic-settings \
    python-multipart unidecode greenlet eval_type_backport

# Copiar config
cp .env.example .env
```

## Uso

```bash
# Arrancar
source .venv/bin/activate
./run.sh
```

O manualmente:

```bash
python -m uvicorn app.main:app --host 0.0.0.0 --port 8000
```

Abrir `http://localhost:8000`

### Cargar datos

1. Ir a `http://localhost:8000/ingestion`
2. Seleccionar rango de fechas
3. Pulsar "Iniciar Ingesta"

O por API:

```bash
# Un día
curl -X POST http://localhost:8000/api/ingestion/trigger-today

# Rango de fechas
curl -X POST http://localhost:8000/api/ingestion/trigger \
  -H "Content-Type: application/json" \
  -d '{"fecha_desde":"2025-01-01","fecha_hasta":"2025-02-15"}'
```

### Backfill histórico

```bash
python scripts/backfill_borme.py 2025-01-01 2025-02-15
```

## API

| Endpoint | Descripción |
|----------|-------------|
| `GET /api/search?q=...&provincia=...` | Buscar empresas |
| `GET /api/companies/{id}` | Detalle de empresa |
| `POST /api/export/csv` | Exportar CSV |
| `POST /api/export/excel` | Exportar Excel |
| `GET /api/stats` | Estadísticas del dashboard |
| `POST /api/ingestion/trigger` | Lanzar ingesta |
| `GET /api/ingestion/status` | Estado de ingesta |
| `GET /docs` | Documentación OpenAPI |

## Rendimiento

Con un solo día de BORME (29 PDFs):
- ~2,200 empresas extraídas
- ~3,800 actos mercantiles
- ~1,400 cargos
- Tiempo: ~25 segundos

## Licencia

MIT
