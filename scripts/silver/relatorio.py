import pandas as pd
from scripts.silver.leitura_bronze import ler_data_raw_csv

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

if __name__ == "__main__":
    df = ler_data_raw_csv()
    print(_calcular_relatorio(df))