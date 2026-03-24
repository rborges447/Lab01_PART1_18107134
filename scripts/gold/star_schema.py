"""
Carga analítica em star schema (PostgreSQL) a partir do DataFrame Silver.

Estratégia de reexecução:
- DDL: CREATE TABLE IF NOT EXISTS (idempotente).
- Dimensões: upsert por chave natural (INSERT ... ON CONFLICT DO UPDATE).
- Fato e ponte: TRUNCATE com RESTART IDENTITY em uma transação e recarga completa
  a partir do snapshot atual do parquet (evita duplicidade na fato ao reprocessar).
"""
from __future__ import annotations

import ast
import re
from typing import Any, Iterator

import numpy as np
import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine

from scripts.utils.config import Settings

# Colunas obrigatórias no parquet Silver para esta carga
REQUIRED_COLUMNS: frozenset[str] = frozenset(
    {
        "id",
        "name",
        "album",
        "album_id",
        "artists",
        "artist_ids",
        "track_number",
        "disc_number",
        "explicit",
        "danceability",
        "energy",
        "key",
        "loudness",
        "mode",
        "speechiness",
        "acousticness",
        "instrumentalness",
        "liveness",
        "valence",
        "tempo",
        "duration_ms",
        "time_signature",
        "year",
        "release_date",
    }
)

_STG_ALBUM = "stg_dim_album"
_STG_ARTIST = "stg_dim_artist"
_STG_DATE = "stg_dim_date"


def _qi(ident: str) -> str:
    """Aspas duplas para identificador SQL (schema/tabela)."""
    if not ident or not re.match(r"^[a-zA-Z_][a-zA-Z0-9_]*$", ident):
        raise ValueError(f"Identificador SQL não suportado ou vazio: {ident!r}")
    return f'"{ident}"'


def _validate_columns(df: pd.DataFrame) -> None:
    missing = REQUIRED_COLUMNS - set(df.columns)
    if missing:
        raise ValueError(
            "DataFrame Silver não contém colunas obrigatórias para o star schema: "
            + ", ".join(sorted(missing))
        )


def _parse_list_cell(val: Any) -> list[Any]:
    """Converte célula (lista ou string tipo literal de lista) em lista Python."""
    if val is None or (isinstance(val, float) and np.isnan(val)):
        return []
    if isinstance(val, list):
        return val
    if isinstance(val, str):
        s = val.strip()
        if not s:
            return []
        try:
            parsed = ast.literal_eval(s)
            return parsed if isinstance(parsed, list) else []
        except (ValueError, SyntaxError):
            return []
    return []


def _pairs_artist_id_name(artist_ids_cell: Any, artists_cell: Any) -> Iterator[tuple[str, str]]:
    ids = _parse_list_cell(artist_ids_cell)
    names = _parse_list_cell(artists_cell)
    n = min(len(ids), len(names))
    for i in range(n):
        yield (str(ids[i]).strip(), str(names[i]).strip())


def _norm_album_id(val: Any) -> str | None:
    if val is None or (isinstance(val, float) and np.isnan(val)):
        return None
    if isinstance(val, float) and val == int(val):
        return str(int(val))
    return str(val).strip()


def _date_sk_from_ts(ts: pd.Timestamp | Any) -> int | None:
    if ts is None or (isinstance(ts, float) and np.isnan(ts)):
        return None
    if pd.isna(ts):
        return None
    if not isinstance(ts, pd.Timestamp):
        ts = pd.Timestamp(ts)
    if pd.isna(ts):
        return None
    return int(ts.strftime("%Y%m%d"))


def _create_tables(engine: Engine, schema: str) -> None:
    sch = _qi(schema)
    ddl_dim_album = f"""
    CREATE TABLE IF NOT EXISTS {sch}.dim_album (
        album_sk BIGSERIAL PRIMARY KEY,
        album_id TEXT NOT NULL UNIQUE,
        album_name TEXT NOT NULL
    );
    """
    ddl_dim_artist = f"""
    CREATE TABLE IF NOT EXISTS {sch}.dim_artist (
        artist_sk BIGSERIAL PRIMARY KEY,
        artist_id TEXT NOT NULL UNIQUE,
        artist_name TEXT NOT NULL
    );
    """
    ddl_dim_date = f"""
    CREATE TABLE IF NOT EXISTS {sch}.dim_date (
        date_sk INTEGER PRIMARY KEY,
        full_date DATE NOT NULL UNIQUE,
        year SMALLINT NOT NULL,
        month SMALLINT NOT NULL,
        day SMALLINT NOT NULL,
        quarter SMALLINT NOT NULL
    );
    """
    ddl_fact = f"""
    CREATE TABLE IF NOT EXISTS {sch}.fact_track_features (
        fact_track_sk BIGSERIAL PRIMARY KEY,
        track_id TEXT NOT NULL UNIQUE,
        track_name TEXT NOT NULL,
        album_sk BIGINT NOT NULL REFERENCES {sch}.dim_album (album_sk),
        date_sk INTEGER REFERENCES {sch}.dim_date (date_sk) ON DELETE SET NULL,
        track_number INTEGER,
        disc_number INTEGER,
        explicit SMALLINT NOT NULL,
        danceability DOUBLE PRECISION,
        energy DOUBLE PRECISION,
        key INTEGER,
        loudness DOUBLE PRECISION,
        mode INTEGER,
        speechiness DOUBLE PRECISION,
        acousticness DOUBLE PRECISION,
        instrumentalness DOUBLE PRECISION,
        liveness DOUBLE PRECISION,
        valence DOUBLE PRECISION,
        tempo DOUBLE PRECISION,
        duration_ms BIGINT,
        time_signature INTEGER,
        year INTEGER
    );
    """
    ddl_bridge = f"""
    CREATE TABLE IF NOT EXISTS {sch}.bridge_track_artist (
        track_id TEXT NOT NULL REFERENCES {sch}.fact_track_features (track_id) ON DELETE CASCADE,
        artist_sk BIGINT NOT NULL REFERENCES {sch}.dim_artist (artist_sk),
        artist_order SMALLINT NOT NULL,
        PRIMARY KEY (track_id, artist_order)
    );
    """
    with engine.begin() as conn:
        for stmt in (ddl_dim_album, ddl_dim_artist, ddl_dim_date, ddl_fact, ddl_bridge):
            conn.execute(text(stmt))
    print(f"[INFO] DDL star schema verificado/criado em {schema}.")


def _drop_staging_simple(engine: Engine, schema: str, name: str) -> None:
    with engine.begin() as conn:
        conn.execute(text(f'DROP TABLE IF EXISTS {_qi(schema)}."{name}"'))


def _upsert_dim_album_from_staging(engine: Engine, schema: str) -> None:
    sch = _qi(schema)
    sql = f"""
    INSERT INTO {sch}.dim_album (album_id, album_name)
    SELECT DISTINCT album_id, album_name FROM {_qi(schema)}."{_STG_ALBUM}"
    ON CONFLICT (album_id) DO UPDATE SET album_name = EXCLUDED.album_name;
    """
    with engine.begin() as conn:
        conn.execute(text(sql))


def _upsert_dim_artist_from_staging(engine: Engine, schema: str) -> None:
    sch = _qi(schema)
    sql = f"""
    INSERT INTO {sch}.dim_artist (artist_id, artist_name)
    SELECT DISTINCT artist_id, artist_name FROM {_qi(schema)}."{_STG_ARTIST}"
    ON CONFLICT (artist_id) DO UPDATE SET artist_name = EXCLUDED.artist_name;
    """
    with engine.begin() as conn:
        conn.execute(text(sql))


def _upsert_dim_date_from_staging(engine: Engine, schema: str) -> None:
    sch = _qi(schema)
    sql = f"""
    INSERT INTO {sch}.dim_date (date_sk, full_date, year, month, day, quarter)
    SELECT DISTINCT date_sk, full_date, year, month, day, quarter
    FROM {_qi(schema)}."{_STG_DATE}"
    ON CONFLICT (date_sk) DO UPDATE SET
        full_date = EXCLUDED.full_date,
        year = EXCLUDED.year,
        month = EXCLUDED.month,
        day = EXCLUDED.day,
        quarter = EXCLUDED.quarter;
    """
    with engine.begin() as conn:
        conn.execute(text(sql))


def _build_staging_album(df: pd.DataFrame, engine: Engine, schema: str) -> None:
    sub = df[["album_id", "album"]].copy()
    sub["album_id"] = sub["album_id"].map(_norm_album_id)
    sub = sub.dropna(subset=["album_id"])
    sub = sub.rename(columns={"album": "album_name"})
    sub = sub.drop_duplicates(subset=["album_id"])
    _drop_staging_simple(engine, schema, _STG_ALBUM)
    sub.to_sql(_STG_ALBUM, engine, schema=schema, if_exists="replace", index=False)
    print(f"[INFO] Staging {_STG_ALBUM}: {len(sub):,} linhas distintas.")


def _build_staging_artist(df: pd.DataFrame, engine: Engine, schema: str) -> None:
    ids_raw = df["artist_ids"].values
    names_raw = df["artists"].values
    rows: list[tuple[str, str]] = []
    mismatch = 0
    for i in range(len(df)):
        ids = _parse_list_cell(ids_raw[i])
        names = _parse_list_cell(names_raw[i])
        if len(ids) != len(names) and (ids or names):
            mismatch += 1
        for aid, aname in _pairs_artist_id_name(ids_raw[i], names_raw[i]):
            if aid and aname:
                rows.append((aid, aname))
    if mismatch:
        print(
            f"[WARN] {mismatch:,} faixas com listas artists/artist_ids de tamanhos diferentes "
            "(pares alinhados até o menor tamanho)."
        )
    art = pd.DataFrame(rows, columns=["artist_id", "artist_name"]).drop_duplicates()
    _drop_staging_simple(engine, schema, _STG_ARTIST)
    art.to_sql(_STG_ARTIST, engine, schema=schema, if_exists="replace", index=False)
    print(f"[INFO] Staging {_STG_ARTIST}: {len(art):,} artistas distintos.")


def _build_staging_date(df: pd.DataFrame, engine: Engine, schema: str) -> None:
    rd = pd.to_datetime(df["release_date"], errors="coerce")
    mask = rd.notna()
    if not mask.any():
        empty = pd.DataFrame(
            columns=["date_sk", "full_date", "year", "month", "day", "quarter"]
        )
        _drop_staging_simple(engine, schema, _STG_DATE)
        empty.to_sql(_STG_DATE, engine, schema=schema, if_exists="replace", index=False)
        print(f"[INFO] Staging {_STG_DATE}: 0 datas válidas.")
        return
    dts = rd[mask].dt.normalize()
    dd = pd.DataFrame(
        {
            "full_date": dts.dt.date,
            "year": dts.dt.year.astype(np.int16),
            "month": dts.dt.month.astype(np.int16),
            "day": dts.dt.day.astype(np.int16),
            "quarter": dts.dt.quarter.astype(np.int16),
        }
    )
    dd["date_sk"] = (dts.dt.year * 10000 + dts.dt.month * 100 + dts.dt.day).astype(int)
    dd = dd.drop_duplicates(subset=["date_sk"])
    _drop_staging_simple(engine, schema, _STG_DATE)
    dd.to_sql(_STG_DATE, engine, schema=schema, if_exists="replace", index=False)
    print(f"[INFO] Staging {_STG_DATE}: {len(dd):,} datas distintas.")


def _fact_dataframe(df: pd.DataFrame, engine: Engine, schema: str) -> pd.DataFrame:
    album_map = pd.read_sql(
        text(f'SELECT album_id, album_sk FROM {_qi(schema)}.dim_album'),
        engine,
    )
    album_map = album_map.assign(album_id_norm=album_map["album_id"].astype(str))[
        ["album_sk", "album_id_norm"]
    ]
    work = df.copy()
    work["album_id_norm"] = work["album_id"].map(_norm_album_id)
    work = work.dropna(subset=["album_id_norm"])
    merged = work.merge(album_map, on="album_id_norm", how="inner")
    if len(merged) < len(work):
        print(
            f"[WARN] {len(work) - len(merged):,} faixas sem album_sk após join "
            "(album_id ausente ou não carregado na dimensão)."
        )

    rd = pd.to_datetime(merged["release_date"], errors="coerce")
    merged["date_sk"] = rd.map(_date_sk_from_ts)

    def _to_int_or_nan(x: Any) -> Any:
        if x is None or (isinstance(x, float) and np.isnan(x)):
            return np.nan
        try:
            return int(x)
        except (TypeError, ValueError):
            return np.nan

    explicit = merged["explicit"].map(lambda x: 1 if bool(x) or x == 1 else 0)

    fact = pd.DataFrame(
        {
            "track_id": merged["id"].astype(str),
            "track_name": merged["name"].astype(str),
            "album_sk": merged["album_sk"].astype(np.int64),
            "date_sk": merged["date_sk"].astype("Int64"),
            "track_number": merged["track_number"].map(_to_int_or_nan).astype("Int64"),
            "disc_number": merged["disc_number"].map(_to_int_or_nan).astype("Int64"),
            "explicit": explicit.astype(np.int16),
            "danceability": pd.to_numeric(merged["danceability"], errors="coerce"),
            "energy": pd.to_numeric(merged["energy"], errors="coerce"),
            "key": merged["key"].map(_to_int_or_nan).astype("Int64"),
            "loudness": pd.to_numeric(merged["loudness"], errors="coerce"),
            "mode": merged["mode"].map(_to_int_or_nan).astype("Int64"),
            "speechiness": pd.to_numeric(merged["speechiness"], errors="coerce"),
            "acousticness": pd.to_numeric(merged["acousticness"], errors="coerce"),
            "instrumentalness": pd.to_numeric(merged["instrumentalness"], errors="coerce"),
            "liveness": pd.to_numeric(merged["liveness"], errors="coerce"),
            "valence": pd.to_numeric(merged["valence"], errors="coerce"),
            "tempo": pd.to_numeric(merged["tempo"], errors="coerce"),
            "duration_ms": pd.to_numeric(merged["duration_ms"], errors="coerce").astype("Int64"),
            "time_signature": merged["time_signature"].map(_to_int_or_nan).astype("Int64"),
            "year": merged["year"].map(_to_int_or_nan).astype("Int64"),
        }
    )
    date_sks = pd.read_sql(
        text(f"SELECT date_sk FROM {_qi(schema)}.dim_date"),
        engine,
    )["date_sk"]
    valid_d = set(date_sks.tolist())
    bad_date = fact["date_sk"].notna() & ~fact["date_sk"].isin(valid_d)
    if bad_date.any():
        n_bad = int(bad_date.sum())
        print(
            f"[WARN] {n_bad:,} faixas com date_sk ausente em dim_date; "
            "date_sk será anulado para respeitar FK."
        )
        fact = fact.copy()
        fact.loc[bad_date, "date_sk"] = pd.NA
    return fact


def _bridge_dataframe(df: pd.DataFrame, engine: Engine, schema: str) -> pd.DataFrame:
    artist_map = pd.read_sql(
        text(f'SELECT artist_id, artist_sk FROM {_qi(schema)}.dim_artist'),
        engine,
    )
    artist_map["artist_id"] = artist_map["artist_id"].astype(str)
    amap = dict(zip(artist_map["artist_id"], artist_map["artist_sk"]))

    track_ids = df["id"].astype(str).values
    ids_raw = df["artist_ids"].values
    names_raw = df["artists"].values
    bridge_rows: list[tuple[str, int, int]] = []
    missing_artist = 0
    for i in range(len(df)):
        tid = str(track_ids[i])
        ids = _parse_list_cell(ids_raw[i])
        names = _parse_list_cell(names_raw[i])
        n = min(len(ids), len(names))
        for ord_ in range(n):
            aid = str(ids[ord_]).strip()
            sk = amap.get(aid)
            if sk is None:
                missing_artist += 1
                continue
            bridge_rows.append((tid, int(sk), ord_ + 1))
    if missing_artist:
        print(f"[WARN] {missing_artist:,} referências de artista ignoradas (id não encontrado na dim).")
    br = pd.DataFrame(bridge_rows, columns=["track_id", "artist_sk", "artist_order"])
    return br.drop_duplicates(subset=["track_id", "artist_order"])


def run_star_schema_load(df: pd.DataFrame, engine: Engine, settings: Settings) -> None:
    """
    Orquestra DDL, carga de dimensões (upsert), truncagem da fato/ponte e recarga.

    Requer ``settings.pg_schema`` definido (validado por ``require_postgres_settings``).
    """
    schema = settings.pg_schema
    if not schema:
        raise ValueError("pg_schema não definido nas settings.")

    _validate_columns(df)
    print(f"[INFO] Iniciando carga star schema ({len(df):,} faixas) em schema {schema!r}.")

    _create_tables(engine, schema)

    _build_staging_album(df, engine, schema)
    _upsert_dim_album_from_staging(engine, schema)

    _build_staging_artist(df, engine, schema)
    _upsert_dim_artist_from_staging(engine, schema)

    _build_staging_date(df, engine, schema)
    _upsert_dim_date_from_staging(engine, schema)

    fact_df = _fact_dataframe(df, engine, schema)
    bridge_df = _bridge_dataframe(df, engine, schema)
    valid_tracks = set(fact_df["track_id"].astype(str))
    bridge_df = bridge_df[bridge_df["track_id"].isin(valid_tracks)]

    sch = _qi(schema)
    with engine.begin() as conn:
        conn.execute(
            text(
                f"TRUNCATE TABLE {sch}.fact_track_features RESTART IDENTITY CASCADE;"
            )
        )

    fact_df.to_sql(
        "fact_track_features",
        engine,
        schema=schema,
        if_exists="append",
        index=False,
        method="multi",
        chunksize=8000,
    )
    print(f"[INFO] Fato carregada: {len(fact_df):,} linhas.")

    bridge_df.to_sql(
        "bridge_track_artist",
        engine,
        schema=schema,
        if_exists="append",
        index=False,
        method="multi",
        chunksize=15000,
    )
    print(f"[INFO] Ponte track-artista: {len(bridge_df):,} linhas.")

    for stg in (_STG_ALBUM, _STG_ARTIST, _STG_DATE):
        _drop_staging_simple(engine, schema, stg)

    print("[DONE] Carga star schema concluída.")
