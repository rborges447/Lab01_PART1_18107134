from __future__ import annotations

from pathlib import Path

from scripts.silver.leitura_bronze import ler_data_raw_csv
from scripts.silver.tratamento import tratar_dados
from scripts.utils.config import load_settings
from scripts.utils.filesystem import criar_pasta_se_nao_existe


def executar_silver() -> Path:
    settings = load_settings()

    df_bronze = ler_data_raw_csv()
    df_tratado = tratar_dados(df_bronze)

    silver_dir = Path(settings.silver_dir)
    criar_pasta_se_nao_existe(silver_dir)

    output_path = silver_dir / "tracks_features_silver.parquet"
    df_tratado.to_parquet(output_path, index=False)

    print(f"[DONE] Silver concluído. Arquivo salvo em: {output_path}")
    print(f"[INFO] Shape final: {df_tratado.shape}")

    return output_path


if __name__ == "__main__":
    executar_silver()

