#!/bin/bash
# FENIX Prospector - Script de arranque
cd "$(dirname "$0")"

# Activar entorno virtual
if [ ! -d ".venv" ]; then
    echo "Error: No se encontró .venv. Ejecuta primero: python3 -m venv .venv && source .venv/bin/activate && pip install -r requirements.txt"
    exit 1
fi

source .venv/bin/activate

echo ""
echo "  ╔══════════════════════════════════════╗"
echo "  ║       FENIX Prospector v0.1.0        ║"
echo "  ║  Lead Prospecting desde BORME/BOE    ║"
echo "  ╚══════════════════════════════════════╝"
echo ""
echo "  Abriendo en: http://localhost:8000"
echo "  Dashboard:   http://localhost:8000/"
echo "  Buscar:      http://localhost:8000/search"
echo "  Ingesta:     http://localhost:8000/ingestion"
echo "  API docs:    http://localhost:8000/docs"
echo ""

# Abrir navegador automáticamente (macOS)
sleep 2 && open http://localhost:8000 &

# Arrancar servidor
python -m uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload
