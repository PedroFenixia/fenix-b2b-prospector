#!/usr/bin/env bash
# ============================================================
# FENIX Prospector — Docker deploy script
# Usage:
#   First time:  ./deploy.sh setup
#   Update:      ./deploy.sh update
#   Logs:        ./deploy.sh logs
#   Stop:        ./deploy.sh stop
#   Backup DB:   ./deploy.sh backup
# ============================================================
set -euo pipefail

COMPOSE="docker compose"
PROJECT="fenix-prospector"

# Colors
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

info()  { echo -e "${GREEN}[INFO]${NC} $*"; }
warn()  { echo -e "${YELLOW}[WARN]${NC} $*"; }
error() { echo -e "${RED}[ERROR]${NC} $*" >&2; }

check_env() {
    if [ ! -f .env ]; then
        error ".env file not found. Copy from .env.example:"
        echo "  cp .env.example .env"
        echo "  nano .env  # fill in production values"
        exit 1
    fi
}

cmd_setup() {
    info "=== First-time setup ==="

    # Install Docker if missing
    if ! command -v docker &>/dev/null; then
        info "Installing Docker..."
        curl -fsSL https://get.docker.com | sh
        systemctl enable --now docker
    fi

    # Install docker compose plugin if missing
    if ! docker compose version &>/dev/null; then
        info "Installing Docker Compose plugin..."
        apt-get update && apt-get install -y docker-compose-plugin
    fi

    check_env

    # Create data directories
    mkdir -p data/borme_pdfs data/exports data/backups

    # Build and start
    info "Building images..."
    $COMPOSE build --no-cache

    info "Starting services..."
    $COMPOSE up -d

    info "Waiting for services to be healthy..."
    sleep 10
    $COMPOSE ps

    info "=== Setup complete ==="
    info "Check status: $COMPOSE ps"
    info "View logs:    $COMPOSE logs -f app"
}

cmd_update() {
    info "=== Updating FENIX Prospector ==="
    check_env

    info "Pulling latest code..."
    git pull --ff-only

    info "Rebuilding app image..."
    $COMPOSE build app

    info "Restarting app (zero-downtime)..."
    $COMPOSE up -d --no-deps app

    info "Waiting for health check..."
    sleep 5

    if $COMPOSE exec app curl -sf http://localhost:8000/health > /dev/null 2>&1; then
        info "Health check passed!"
    else
        warn "Health check did not pass yet. Check logs: $COMPOSE logs app"
    fi

    $COMPOSE ps
    info "=== Update complete ==="
}

cmd_logs() {
    $COMPOSE logs -f --tail=100 "${@:-}"
}

cmd_stop() {
    info "Stopping all services..."
    $COMPOSE down
    info "Stopped."
}

cmd_backup() {
    info "=== Backing up PostgreSQL ==="
    check_env

    BACKUP_DIR="data/backups"
    mkdir -p "$BACKUP_DIR"
    TIMESTAMP=$(date +%Y%m%d_%H%M%S)
    BACKUP_FILE="$BACKUP_DIR/fenix_pg_${TIMESTAMP}.sql.gz"

    $COMPOSE exec -T postgres pg_dump -U fenix fenix_prospector | gzip > "$BACKUP_FILE"

    info "Backup saved: $BACKUP_FILE"

    # Keep only last 7 backups
    ls -t "$BACKUP_DIR"/fenix_pg_*.sql.gz 2>/dev/null | tail -n +8 | xargs -r rm
    info "Old backups cleaned (keeping last 7)."
}

cmd_status() {
    $COMPOSE ps
    echo ""
    info "App health:"
    $COMPOSE exec app curl -sf http://localhost:8000/health 2>/dev/null || echo "  (not responding)"
}

# --- Main ---
case "${1:-help}" in
    setup)  cmd_setup ;;
    update) cmd_update ;;
    logs)   shift; cmd_logs "$@" ;;
    stop)   cmd_stop ;;
    backup) cmd_backup ;;
    status) cmd_status ;;
    *)
        echo "FENIX Prospector — Deploy Script"
        echo ""
        echo "Usage: $0 <command>"
        echo ""
        echo "Commands:"
        echo "  setup   First-time install (Docker + build + start)"
        echo "  update  Pull code, rebuild, restart app"
        echo "  logs    Show container logs (pass service name to filter)"
        echo "  stop    Stop all containers"
        echo "  backup  Backup PostgreSQL to data/backups/"
        echo "  status  Show service status"
        ;;
esac
