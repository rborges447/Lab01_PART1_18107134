from __future__ import annotations

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from scripts.utils.config import Settings


def _build_db_url(settings: Settings, database: str) -> str:
    if (
        settings.pg_host is None
        or settings.pg_port is None
        or settings.pg_user is None
        or settings.pg_password is None
    ):
        raise ValueError(
            "PostgreSQL não configurado. Defina PG_HOST, PG_PORT, PG_USER e PG_PASSWORD no .env "
            "para o pipeline Gold."
        )
    return (
        f"postgresql+psycopg://{settings.pg_user}:{settings.pg_password}"
        f"@{settings.pg_host}:{settings.pg_port}/{database}"
    )


def get_admin_engine(settings: Settings) -> Engine:
    # Banco administrativo padrão para criação/checagem do database alvo.
    return create_engine(_build_db_url(settings, "postgres"), isolation_level="AUTOCOMMIT")


def get_target_engine(settings: Settings) -> Engine:
    if settings.pg_database is None:
        raise ValueError("PG_DATABASE não configurado no .env.")
    return create_engine(_build_db_url(settings, settings.pg_database))


def ensure_database_exists(settings: Settings) -> None:
    with get_admin_engine(settings).connect() as conn:
        exists = conn.execute(
            text("SELECT 1 FROM pg_database WHERE datname = :db_name"),
            {"db_name": settings.pg_database},
        ).scalar()

        if exists:
            print(f"[INFO] Database já existe: {settings.pg_database}")
            return

        conn.execute(text(f'CREATE DATABASE "{settings.pg_database}"'))
        print(f"[INFO] Database criado: {settings.pg_database}")


def ensure_schema_exists(engine: Engine, schema_name: str) -> None:
    with engine.begin() as conn:
        conn.execute(text(f'CREATE SCHEMA IF NOT EXISTS "{schema_name}"'))
    print(f"[INFO] Schema garantido: {schema_name}")

