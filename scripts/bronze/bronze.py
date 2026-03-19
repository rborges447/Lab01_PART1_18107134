from __future__ import annotations

from scripts.utils.config import load_settings

from scripts.bronze.extract import descompactar_zip
from scripts.utils.filesystem import criar_pasta_se_nao_existe
from scripts.bronze.kaggle_client import baixar_dataset


def run_bronze() -> None:
    settings = load_settings()

    raw_dir = settings.raw_dir
    criar_pasta_se_nao_existe(raw_dir)
    baixar_dataset(settings.dataset, raw_dir)
    descompactar_zip(raw_dir)

    print(f"[DONE] Bronze concluído. Arquivos salvos em: {raw_dir}")


if __name__ == "__main__":
    run_bronze()

