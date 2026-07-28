"""Configuração central do projeto, carregada de variáveis de ambiente (12-factor)."""

from pathlib import Path

from pydantic_settings import BaseSettings, SettingsConfigDict

REPO_ROOT = Path(__file__).resolve().parents[2]


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    # Camadas do data lake local (medalhão)
    data_dir: Path = REPO_ROOT / "data"
    bronze_dir: Path = REPO_ROOT / "data" / "bronze"
    work_dir: Path = REPO_ROOT / "data" / "_work"
    silver_dir: Path = REPO_ROOT / "data" / "silver"
    logs_dir: Path = REPO_ROOT / "logs"

    # Fonte dos dados (Fase 1: extraction/rf_client.py).
    # TODO: confirmar a URL oficial atual do portal de Dados Abertos do CNPJ e preencher
    # via variável de ambiente RF_BASE_URL — não deve ser hardcoded/adivinhada aqui.
    rf_base_url: str = ""

    # Postgres — camada gold (Fase 2)
    postgres_dsn: str = "postgresql://cnpj:cnpj@localhost:5432/cnpj"


settings = Settings()
