from pathlib import Path
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    # Paths
    base_dir: Path = Path(__file__).resolve().parent.parent
    data_dir: Path = Path("data")
    borme_pdf_dir: Path = Path("data/borme_pdfs")
    export_dir: Path = Path("data/exports")

    # Database
    database_url: str = "postgresql+asyncpg://fenix:F3n1x!PG2026@localhost:5432/fenix_prospector"
    db_pool_size: int = 10
    db_max_overflow: int = 20
    pg_fts_config: str = "fenix_spanish"

    # BOE API
    boe_api_base: str = "https://boe.es/datosabiertos/api"

    # Ingestion
    pdf_download_concurrency: int = 15

    # Scheduler (activar con SCHEDULER_ENABLED=true en .env)
    scheduler_enabled: bool = False
    scheduler_hour: int = 10
    scheduler_minute: int = 0

    # CIF Enrichment
    apiempresas_key: str = ""
    # Proxies para enriquecimiento (separados por coma). Se rotan automáticamente.
    # Ej: socks5://127.0.0.1:1080,socks5://127.0.0.1:1081,http://user:pass@proxy:8080
    enrichment_proxies: str = ""

    # Auth
    admin_password: str = "FenixIA360!"
    demo_password: str = "fenixiaprospector"
    secret_key: str = "fenix-b2b-secret-change-me-in-production"

    # Stripe (procesador de pagos)
    stripe_secret_key: str = ""
    stripe_webhook_secret: str = ""
    stripe_price_pro: str = ""  # price_xxx for Pro plan
    stripe_price_enterprise: str = ""  # price_xxx for Enterprise plan
    app_url: str = "https://b2b.fenixia.tech"

    # RevenueCat (gestion de suscripciones)
    revenuecat_api_key: str = ""  # Secret API key (sk_xxx)
    revenuecat_webhook_auth: str = ""  # Authorization header for incoming webhooks
    revenuecat_entitlement_pro: str = "pro"  # Entitlement ID for Pro
    revenuecat_entitlement_enterprise: str = "enterprise"  # Entitlement ID for Enterprise

    # Typesense (motor de busqueda)
    typesense_url: str = "http://localhost:8108"
    typesense_api_key: str = "fenix-ts-local-key"
    typesense_collection: str = "companies"

    # Email (SMTP for verification)
    smtp_host: str = ""
    smtp_port: int = 587
    smtp_user: str = ""
    smtp_password: str = ""
    smtp_from: str = "noreply@fenixia.tech"
    smtp_from_name: str = "FENIX Prospector"

    model_config = {"env_file": ".env", "env_file_encoding": "utf-8"}

    def ensure_dirs(self) -> None:
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.borme_pdf_dir.mkdir(parents=True, exist_ok=True)
        self.export_dir.mkdir(parents=True, exist_ok=True)


settings = Settings()
