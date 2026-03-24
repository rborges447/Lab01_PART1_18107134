from __future__ import annotations

from pathlib import Path

import pandas as pd

from scripts.gold.db import (
    ensure_database_exists,
    ensure_schema_exists,
    get_target_engine,
)
from scripts.gold.star_schema import run_star_schema_load
from scripts.utils.config import load_settings, require_postgres_settings


def run_gold_pipeline() -> Path:
    settings = load_settings()
    require_postgres_settings(settings)

    parquet_path = Path(settings.silver_dir) / settings.silver_parquet_name
    if not parquet_path.exists():
        raise FileNotFoundError(f"Parquet Silver não encontrado: {parquet_path}")

    print(f"[INFO] Lendo parquet Silver: {parquet_path}")
    df = pd.read_parquet(parquet_path)

    ensure_database_exists(settings)
    engine = get_target_engine(settings)
    ensure_schema_exists(engine, settings.pg_schema)

    run_star_schema_load(df, engine, settings)

    print(
        f"[DONE] Gold concluído (star schema em {settings.pg_schema}): "
        f"{len(df):,} faixas no parquet de origem"
    )

    return parquet_path
