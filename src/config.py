from __future__ import annotations

from functools import lru_cache

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    database_url: str = Field(
        default="postgresql+asyncpg://glacis:glacis@localhost:5432/glacis",
        alias="DATABASE_URL",
    )
    db_ingest_statement_timeout_ms: int = Field(default=300, alias="DB_INGEST_STATEMENT_TIMEOUT_MS")
    ingest_backlog_threshold: int = Field(default=10_000, alias="INGEST_BACKLOG_THRESHOLD")

    openai_api_key: str | None = Field(default=None, alias="OPENAI_API_KEY")
    openai_classifier_model: str = Field(default="gpt-4.1-mini", alias="OPENAI_CLASSIFIER_MODEL")
    openai_extractor_model: str = Field(default="gpt-4.1", alias="OPENAI_EXTRACTOR_MODEL")

    worker_concurrency: int = Field(default=8, alias="WORKER_CONCURRENCY")
    llm_global_rate_per_sec: float = Field(default=50.0, alias="LLM_GLOBAL_RATE_PER_SEC")

    hmac_verify_enabled: bool = Field(default=False, alias="HMAC_VERIFY_ENABLED")


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()
