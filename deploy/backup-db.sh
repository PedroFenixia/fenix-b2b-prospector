#!/bin/bash
# Backup de la base de datos SQLite de FENIX B2B Prospector
# Ejecutar via cron: 0 3 * * * /opt/fenix-b2b/deploy/backup-db.sh

BACKUP_DIR="/opt/fenix-b2b/backups"
DB_PATH="/opt/fenix-b2b/data/prospector.db"
DATE=$(date +%Y%m%d_%H%M%S)
KEEP_DAYS=30

mkdir -p "$BACKUP_DIR"

# Usar sqlite3 .backup para copia segura (no bloquea escrituras)
if [ -f "$DB_PATH" ]; then
    sqlite3 "$DB_PATH" ".backup '$BACKUP_DIR/prospector_${DATE}.db'"

    # Comprimir
    gzip "$BACKUP_DIR/prospector_${DATE}.db"

    echo "[$(date)] Backup completado: prospector_${DATE}.db.gz"

    # Eliminar backups antiguos (mas de N dias)
    find "$BACKUP_DIR" -name "prospector_*.db.gz" -mtime +${KEEP_DAYS} -delete
    echo "[$(date)] Backups antiguos eliminados (>${KEEP_DAYS} dias)"
else
    echo "[$(date)] ERROR: Base de datos no encontrada en $DB_PATH"
    exit 1
fi
