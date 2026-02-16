from pathlib import Path
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    # Paths
    base_dir: Path = Path(__file__).resolve().parent.parent
    data_dir: Path = Path("data")
    borme_pdf_dir: Path = Path("data/borme_pdfs")
    export_dir: Path = Path("data/exports")

    # Database
    database_url: str = "sqlite+aiosqlite:///data/prospector.db"

    # BOE API
    boe_api_base: str = "https://boe.es/datosabiertos/api"

    # Ingestion
    pdf_download_concurrency: int = 15

    # Scheduler (activar con SCHEDULER_ENABLED=true en .env)
    scheduler_enabled: bool = False
    scheduler_hour: int = 10
    scheduler_minute: int = 0

    # CIF Enrichment (APIEmpresas.es - plan Sandbox gratuito)
    apiempresas_key: str = ""

    # Auth
    admin_password: str = "FenixIA360!"
    demo_password: str = "fenixiaprospector"
    secret_key: str = "fenix-b2b-secret-change-me-in-production"

    # Stripe
    stripe_secret_key: str = ""
    stripe_webhook_secret: str = ""
    stripe_price_pro: str = ""  # price_xxx for Pro plan
    stripe_price_enterprise: str = ""  # price_xxx for Enterprise plan
    app_url: str = "https://b2b.fenixia.tech"

    model_config = {"env_file": ".env", "env_file_encoding": "utf-8"}

    def ensure_dirs(self) -> None:
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.borme_pdf_dir.mkdir(parents=True, exist_ok=True)
        self.export_dir.mkdir(parents=True, exist_ok=True)


settings = Settings()
