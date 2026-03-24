from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Optional


REPO_ROOT = Path(__file__).resolve().parent.parent.parent
ENV_PATH_DEFAULT = REPO_ROOT / ".env"


@dataclass(frozen=True)
class Settings:
    dataset: str
    raw_dir: str
    docs_dir: str
    silver_dir: str
    silver_parquet_name: str
    silver_scatter_sample_size: int
    # Opcionais: ausentes no .env para pipelines que não usam Postgres (ex.: Silver/Bronze).
    pg_host: Optional[str]
    pg_port: Optional[int]
    pg_user: Optional[str]
    pg_password: Optional[str]
    pg_database: Optional[str]
    pg_schema: Optional[str]
    pg_table: Optional[str]


def _parse_env_file(path: Path) -> Dict[str, str]:
    if not path.exists():
        raise FileNotFoundError(
            f"Arquivo .env não encontrado em: {path}. Crie o arquivo com DATASET e RAW_DIR."
        )

    out: Dict[str, str] = {}
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if "=" not in line:
            continue

        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip()

        if (value.startswith('"') and value.endswith('"')) or (
            value.startswith("'") and value.endswith("'")
        ):
            value = value[1:-1]

        out[key] = value

    return out


def _resolve_path(path_value: str) -> Path:
    path = Path(path_value)
    if not path.is_absolute():
        path = (REPO_ROOT / path).resolve()
    return path


def load_settings(env_path: Optional[Path] = None) -> Settings:
    env_path = env_path or ENV_PATH_DEFAULT
    env = _parse_env_file(env_path)

    if "DATASET" not in env:
        raise KeyError("Chave DATASET não encontrada no .env.")
    if "RAW_DIR" not in env:
        raise KeyError("Chave RAW_DIR não encontrada no .env.")
    if "SILVER_DIR" not in env:
        raise KeyError("Chave SILVER_DIR não encontrada no .env.")

    dataset = env["DATASET"]
    raw_dir = _resolve_path(env["RAW_DIR"])
    silver_dir = _resolve_path(env["SILVER_DIR"])
    docs_dir_value = env.get("DOCS_DIR", "docs")
    docs_dir = _resolve_path(docs_dir_value)
    silver_parquet_name = env.get("SILVER_PARQUET_NAME", "tracks_features_silver.parquet")
    silver_scatter_sample_size = int(env.get("SILVER_SCATTER_SAMPLE_SIZE", "50000"))

    if env.get("PG_HOST"):
        if "PG_PORT" not in env:
            raise KeyError("Chave PG_PORT não encontrada no .env (PG_HOST está definido).")
        if "PG_USER" not in env:
            raise KeyError("Chave PG_USER não encontrada no .env (PG_HOST está definido).")
        if "PG_PASSWORD" not in env:
            raise KeyError("Chave PG_PASSWORD não encontrada no .env (PG_HOST está definido).")
        if "PG_DATABASE" not in env:
            raise KeyError("Chave PG_DATABASE não encontrada no .env (PG_HOST está definido).")
        pg_host = env["PG_HOST"]
        pg_port = int(env["PG_PORT"])
        pg_user = env["PG_USER"]
        pg_password = env["PG_PASSWORD"]
        pg_database = env["PG_DATABASE"]
        pg_schema = env.get("PG_SCHEMA", "public")
        pg_table = env.get("PG_TABLE")
    else:
        pg_host = None
        pg_port = None
        pg_user = None
        pg_password = None
        pg_database = None
        pg_schema = None
        pg_table = None

    return Settings(
        dataset=dataset,
        raw_dir=str(raw_dir),
        docs_dir=str(docs_dir),
        silver_dir=str(silver_dir),
        silver_parquet_name=silver_parquet_name,
        silver_scatter_sample_size=silver_scatter_sample_size,
        pg_host=pg_host,
        pg_port=pg_port,
        pg_user=pg_user,
        pg_password=pg_password,
        pg_database=pg_database,
        pg_schema=pg_schema,
        pg_table=pg_table,
    )


def require_postgres_settings(settings: Settings) -> None:
    """
    Garante que o .env define Postgres completo. Obrigatório para o pipeline Gold;
    Silver e Bronze não precisam de PG_*.

    ``PG_TABLE`` é opcional: o Gold em star schema usa nomes fixos de tabela no ``PG_SCHEMA``.
    """
    missing: list[str] = []
    if not settings.pg_host:
        missing.append("PG_HOST")
    if settings.pg_port is None:
        missing.append("PG_PORT")
    if not settings.pg_user:
        missing.append("PG_USER")
    if settings.pg_password is None:
        missing.append("PG_PASSWORD")
    if not settings.pg_database:
        missing.append("PG_DATABASE")
    if not settings.pg_schema:
        missing.append("PG_SCHEMA")
    if missing:
        raise ValueError(
            "Pipeline Gold exige PostgreSQL no .env. Defina: " + ", ".join(missing)
        )
