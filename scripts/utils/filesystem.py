from __future__ import annotations

from pathlib import Path


def criar_pasta_se_nao_existe(path: str | Path) -> None:
    p = Path(path)
    if not p.exists():
        p.mkdir(parents=True, exist_ok=True)
        print(f"[INFO] Pasta criada: {p}")
    else:
        print(f"[INFO] Pasta já existe: {p}")

