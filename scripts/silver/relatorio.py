from __future__ import annotations

from datetime import datetime
from pathlib import Path

import pandas as pd

from scripts.silver.leitura_bronze import ler_data_raw_csv
from scripts.utils.config import load_settings
from  scripts.utils.filesystem import criar_pasta_se_nao_existe

def _calcular_relatorio(df: pd.DataFrame) -> dict:
    total_linhas, total_colunas = df.shape
 
    # 1. Tipos de colunas
    tipos = df.dtypes.rename("tipo").reset_index()
    tipos.columns = ["coluna", "tipo"]
    tipos["tipo"] = tipos["tipo"].astype(str)
 
    # 2. Contagem de nulos
    nulos = df.isnull().sum().reset_index()
    nulos.columns = ["coluna", "nulos"]
    nulos["pct_nulos"] = (nulos["nulos"] / total_linhas * 100).round(2)
 
    # 3. Estatísticas descritivas — apenas colunas numéricas
    numericas = df.select_dtypes(include="number")
    if not numericas.empty:
        stats = numericas.agg(["mean", "std", "min", "median", "max"]).T.reset_index()
        stats.columns = ["coluna", "media", "desvio_padrao", "min", "mediana", "max"]
        stats = stats.round(4)
    else:
        stats = pd.DataFrame(columns=["coluna", "media", "desvio_padrao", "min", "mediana", "max"])
 
    return {
        "total_linhas":   total_linhas,
        "total_colunas":  total_colunas,
        "tipos":          tipos,
        "nulos":          nulos,
        "stats":          stats,
    }

def _df_to_md(df: pd.DataFrame) -> str:
    """
    Converte um DataFrame em tabela Markdown com colunas alinhadas.
    Cada célula é preenchida com espaços até o comprimento máximo da coluna,
    garantindo alinhamento visual no editor.
    """
    df_str = df.astype(str)

    # Largura máxima de cada coluna: maior entre o header e os valores
    col_widths = {
        col: max(len(col), df_str[col].str.len().max())
        for col in df_str.columns
    }

    def formatar_linha(valores: list) -> str:
        celulas = [str(v).ljust(col_widths[col]) for v, col in zip(valores, df_str.columns)]
        return "| " + " | ".join(celulas) + " |"

    header    = formatar_linha(df_str.columns.tolist())
    separator = "| " + " | ".join(["-" * col_widths[col] for col in df_str.columns]) + " |"
    rows      = [formatar_linha(row) for row in df_str.itertuples(index=False)]

    return "\n".join([header, separator] + rows)

def _salvar_markdown(relatorio: dict, nome_dataset: str) -> str:
    settings = load_settings()

    docs_dir = Path(settings.docs_dir)
    criar_pasta_se_nao_existe(docs_dir)

    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    nome_arquivo = f"data_quality_report_{nome_dataset}.md"
    caminho_md = docs_dir / nome_arquivo

    linhas = []

    linhas += [
        f"# Data Quality Report — {nome_dataset}",
        f"\n> Gerado automaticamente em {timestamp}",
        "\n---\n",
    ]

    linhas += [
        "## 1. Visão Geral\n",
        "| | |",
        "|---|---|",
        f"| Total de linhas  | {relatorio['total_linhas']:,} |",
        f"| Total de colunas | {relatorio['total_colunas']} |",
        "\n---\n",
    ]

    linhas += ["## 2. Tipos de Colunas\n"]
    linhas += [_df_to_md(relatorio["tipos"])]
    linhas += ["\n---\n"]

    linhas += ["## 3. Contagem de Nulos\n"]
    linhas += [_df_to_md(relatorio["nulos"])]
    linhas += ["\n---\n"]

    linhas += ["## 4. Estatísticas Descritivas (colunas numéricas)\n"]
    if relatorio["stats"].empty:
        linhas += ["> Nenhuma coluna numérica encontrada.\n"]
    else:
        linhas += [_df_to_md(relatorio["stats"])]
    linhas += ["\n---\n"]

    linhas += [
        "## 5. Gráficos\n",
        "> *(a ser preenchido pelo script Silver)*\n",
    ]

    caminho_md.write_text("\n".join(linhas), encoding="utf-8")

    print(f"[INFO] Relatório salvo em: {caminho_md}")
    return str(caminho_md)

def  gerar_relatorio(df: pd.DataFrame):
    relatorio = _calcular_relatorio(df)
    _salvar_markdown(relatorio, "spotify-12m-songs")


if __name__ == "__main__":
    df = ler_data_raw_csv()
    gerar_relatorio(df)