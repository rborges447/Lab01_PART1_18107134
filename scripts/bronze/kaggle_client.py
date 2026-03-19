from __future__ import annotations

import json
from pathlib import Path
from typing import Dict, Optional, Tuple


def _parse_env_file(path: Path) -> Dict[str, str]:
    if not path.exists():
        return {}

    out: Dict[str, str] = {}
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
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


def _load_kaggle_credentials() -> Tuple[Optional[str], Optional[str]]:
    repo_root = Path(__file__).resolve().parent.parent.parent
    env_path = repo_root / ".env"
    env = _parse_env_file(env_path)

    username = env.get("KAGGLE_USERNAME")
    key = env.get("KAGGLE_KEY")

    if username is not None and not username.strip():
        username = None
    if key is not None and not key.strip():
        key = None

    return username, key


def _configure_kaggle_json_from_env() -> None:
    username, key = _load_kaggle_credentials()
    if not username or not key:
        # Se não estiver configurado, deixa o kaggle falhar do jeito padrão.
        return

    kaggle_dir = Path.home() / ".kaggle"
    kaggle_dir.mkdir(parents=True, exist_ok=True)

    kaggle_json_path = kaggle_dir / "kaggle.json"
    payload = {"username": username, "key": key}
    kaggle_json_path.write_text(json.dumps(payload), encoding="utf-8")

    # Não imprima credenciais.
    print("[INFO] Kaggle `kaggle.json` gerado a partir do `.env`.")


def baixar_dataset(dataset: str, destino: str) -> None:
    _configure_kaggle_json_from_env()

    import kaggle

    print(f"[INFO] Baixando dataset '{dataset}' do Kaggle...")
    kaggle.api.dataset_download_files(
        dataset,
        path=destino,
        unzip=False,  # descompactaremos manualmente para manter o controle do fluxo
    )
    print(f"[INFO] Download concluído em: {destino}")

