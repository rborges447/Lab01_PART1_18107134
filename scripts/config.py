from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Optional


REPO_ROOT = Path(__file__).resolve().parent.parent
ENV_PATH_DEFAULT = REPO_ROOT / ".env"


@dataclass(frozen=True)
class Settings:
    dataset: str
    raw_dir: str  # caminho absoluto resolvido


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

        # remove aspas simples/duplas comuns em .env
        if (value.startswith('"') and value.endswith('"')) or (
            value.startswith("'") and value.endswith("'")
        ):
            value = value[1:-1]

        out[key] = value

    return out


def load_settings(env_path: Optional[Path] = None) -> Settings:
    env_path = env_path or ENV_PATH_DEFAULT
    env = _parse_env_file(env_path)

    if "DATASET" not in env:
        raise KeyError("Chave DATASET não encontrada no .env.")
    if "RAW_DIR" not in env:
        raise KeyError("Chave RAW_DIR não encontrada no .env.")

    dataset = env["DATASET"]
    raw_dir_value = env["RAW_DIR"]

    raw_path = Path(raw_dir_value)
    if not raw_path.is_absolute():
        raw_path = (REPO_ROOT / raw_path).resolve()

    return Settings(dataset=dataset, raw_dir=str(raw_path))

