import ast

import pandas as pd


def log_shape(df: pd.DataFrame, etapa: str) -> None:
    print(f"[INFO] {etapa}: {df.shape[0]:,} linhas x {df.shape[1]} colunas")


def _parse_primeiro_artista(valor: str) -> str:
    """
    Extrai o primeiro artista de uma string no formato "['Artist A', 'Artist B']".
    Retorna o valor original se o parse falhar.
    """
    try:
        lista = ast.literal_eval(valor)
        if isinstance(lista, list) and len(lista) > 0:
            return lista[0]
    except (ValueError, SyntaxError):
        pass
    return valor


def remover_duplicatas(df: pd.DataFrame) -> pd.DataFrame:
    antes = len(df)
    df = df.drop_duplicates(subset="id")
    depois = len(df)
    print(f"[INFO] Duplicatas removidas: {antes - depois:,}")
    return df


def remover_nulos(df: pd.DataFrame) -> pd.DataFrame:
    antes = len(df)
    df = df.dropna(subset=["name", "album"])
    depois = len(df)
    print(f"[INFO] Linhas removidas por nulos em 'name'/'album': {antes - depois:,}")
    return df


def converter_tipos(df: pd.DataFrame) -> pd.DataFrame:
    # release_date: string -> datetime (erros viram NaT)
    df["release_date"] = pd.to_datetime(df["release_date"], errors="coerce")
    print(
        "[INFO] 'release_date' convertida para datetime "
        f"({df['release_date'].isna().sum():,} valores inválidos viraram NaT)"
    )

    # explicit: bool -> int (0/1)
    df["explicit"] = df["explicit"].astype(int)
    print("[INFO] 'explicit' convertida para int (0/1)")

    return df


def filtrar_invalidos(df: pd.DataFrame) -> pd.DataFrame:
    antes = len(df)

    # year == 0 é inválido
    df = df[df["year"] > 0]

    # tempo == 0 indica dado corrompido
    df = df[df["tempo"] > 0]

    # duration_ms: remove outliers acima do percentil 99.9
    p999 = df["duration_ms"].quantile(0.999)
    df = df[df["duration_ms"] <= p999]

    depois = len(df)
    print(
        "[INFO] Registros inválidos removidos (year=0, tempo=0, duration extremo): "
        f"{antes - depois:,}"
    )
    return df


def enriquecer(df: pd.DataFrame) -> pd.DataFrame:
    # Extrai primeiro artista da lista em string
    df["artist_name"] = df["artists"].apply(_parse_primeiro_artista)
    print("[INFO] Coluna 'artist_name' criada (primeiro artista extraído)")

    # Década a partir do ano
    df["decade"] = (df["year"] // 10 * 10).astype(int)
    print("[INFO] Coluna 'decade' criada")

    # Duração em minutos
    df["duration_min"] = (df["duration_ms"] / 60_000).round(2)
    print("[INFO] Coluna 'duration_min' criada")

    return df


def tratar_dados(df: pd.DataFrame) -> pd.DataFrame:
    """
    Executa o pipeline Silver completo a partir de um DataFrame bruto.
    """
    if df.empty:
        print("[WARN] DataFrame de entrada está vazio. Nada a tratar.")
        return df

    log_shape(df, "Entrada")
    df = remover_duplicatas(df)
    log_shape(df, "Após remover duplicatas")

    df = remover_nulos(df)
    log_shape(df, "Após remover nulos")

    df = converter_tipos(df)
    log_shape(df, "Após converter tipos")

    df = filtrar_invalidos(df)
    log_shape(df, "Após filtrar inválidos")

    df = enriquecer(df)
    log_shape(df, "Após enriquecimento")

    return df
import os
import ast
import pandas as pd


 



def log_shape(df: pd.DataFrame, etapa: str) -> None:
    print(f"[INFO] {etapa}: {df.shape[0]:,} linhas x {df.shape[1]} colunas")
 
 
def _parse_primeiro_artista(valor: str) -> str:
    """
    Extrai o primeiro artista de uma string no formato "['Artist A', 'Artist B']".
    Retorna o valor original se o parse falhar.
    """
    try:
        lista = ast.literal_eval(valor)
        if isinstance(lista, list) and len(lista) > 0:
            return lista[0]
    except (ValueError, SyntaxError):
        pass
    return valor
 
 
# ── Pipeline de limpeza ───────────────────────────────────────────────────────
 
def carregar_bronze(raw_dir: str, filename: str) -> pd.DataFrame:
    caminho = os.path.join(raw_dir, filename)
    print(f"[INFO] Lendo: {caminho}")
    df = pd.read_csv(caminho)
    log_shape(df, "Bronze carregado")
    return df
 
 
def remover_duplicatas(df: pd.DataFrame) -> pd.DataFrame:
    antes = len(df)
    df = df.drop_duplicates(subset="id")
    depois = len(df)
    print(f"[INFO] Duplicatas removidas: {antes - depois:,}")
    return df
 
 
def remover_nulos(df: pd.DataFrame) -> pd.DataFrame:
    antes = len(df)
    df = df.dropna(subset=["name", "album"])
    depois = len(df)
    print(f"[INFO] Linhas removidas por nulos em 'name'/'album': {antes - depois:,}")
    return df
 
 
def converter_tipos(df: pd.DataFrame) -> pd.DataFrame:
    # release_date: string -> datetime (erros viram NaT)
    df["release_date"] = pd.to_datetime(df["release_date"], errors="coerce")
    print(f"[INFO] 'release_date' convertida para datetime "
          f"({df['release_date'].isna().sum():,} valores inválidos viraram NaT)")
 
    # explicit: bool -> int (0/1)
    df["explicit"] = df["explicit"].astype(int)
    print(f"[INFO] 'explicit' convertida para int (0/1)")
 
    return df
 
 
def filtrar_invalidos(df: pd.DataFrame) -> pd.DataFrame:
    antes = len(df)
 
    # year == 0 é inválido
    df = df[df["year"] > 0]
 
    # tempo == 0 indica dado corrompido
    df = df[df["tempo"] > 0]
 
    # duration_ms: remove outliers acima do percentil 99.9
    p999 = df["duration_ms"].quantile(0.999)
    df = df[df["duration_ms"] <= p999]
 
    depois = len(df)
    print(f"[INFO] Registros inválidos removidos (year=0, tempo=0, duration extremo): "
          f"{antes - depois:,}")
    return df
 
 
def enriquecer(df: pd.DataFrame) -> pd.DataFrame:
    # Extrai primeiro artista da lista em string
    df["artist_name"] = df["artists"].apply(_parse_primeiro_artista)
    print(f"[INFO] Coluna 'artist_name' criada (primeiro artista extraído)")
 
    # Década a partir do ano
    df["decade"] = (df["year"] // 10 * 10).astype(int)
    print(f"[INFO] Coluna 'decade' criada")
 
    # Duração em minutos
    df["duration_min"] = (df["duration_ms"] / 60_000).round(2)
    print(f"[INFO] Coluna 'duration_min' criada")
 
    return df