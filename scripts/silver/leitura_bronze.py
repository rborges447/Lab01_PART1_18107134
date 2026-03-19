from __future__ import annotations

from pathlib import Path
import pandas as pd

from scripts.utils.config import load_settings


def ler_data_raw_csv() -> pd.DataFrame:
    """
    Lê o primeiro CSV encontrado em `data/raw/` e retorna um DataFrame.
    Se a pasta não existir ou estiver vazia, avisa e retorna DataFrame vazio.
    """
    settings = load_settings()
    raw_dir = Path(settings.raw_dir)

    if not raw_dir.exists():
        print(f"[WARN] Pasta não existe: {raw_dir}")
        return pd.DataFrame()

    csv_paths = sorted(raw_dir.glob("*.csv"))

    if not csv_paths:
        print(f"[WARN] Pasta existe, mas está vazia (sem CSV em: {raw_dir})")
        return pd.DataFrame()

    p = csv_paths[0]
    df = pd.read_csv(p)

    if df.empty:
        print(f"[WARN] CSV vazio: {p.name}")

    df["__source"] = p.name
    return df


if __name__ == "__main__":
    df = ler_data_raw_csv()
    print(f"[INFO] df shape: {df.shape}")

