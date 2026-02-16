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
    pdf_download_concurrency: int = 5

    # Scheduler (activar con SCHEDULER_ENABLED=true en .env)
    scheduler_enabled: bool = False
    scheduler_hour: int = 10
    scheduler_minute: int = 0

    # Auth
    admin_user: str = "admin"
    admin_password: str = "fenix2024"
    secret_key: str = "fenix-b2b-secret-change-me-in-production"

    model_config = {"env_file": ".env", "env_file_encoding": "utf-8"}

    def ensure_dirs(self) -> None:
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.borme_pdf_dir.mkdir(parents=True, exist_ok=True)
        self.export_dir.mkdir(parents=True, exist_ok=True)


settings = Settings()
