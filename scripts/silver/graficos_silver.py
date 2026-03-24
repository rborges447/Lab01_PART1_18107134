from __future__ import annotations

from datetime import datetime
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from scripts.utils.config import Settings
from scripts.utils.filesystem import criar_pasta_se_nao_existe

_CORR_CANDIDATES = [
    "danceability",
    "energy",
    "loudness",
    "speechiness",
    "acousticness",
    "instrumentalness",
    "liveness",
    "valence",
    "tempo",
]


def _fig_save(path: Path) -> None:
    plt.tight_layout()
    plt.savefig(path, dpi=120, bbox_inches="tight")
    plt.close()
    print(f"[INFO] Gráfico salvo: {path}")


def _chart_histogram_tempo(df: pd.DataFrame, out: Path) -> bool:
    if "tempo" not in df.columns:
        return False
    s = pd.to_numeric(df["tempo"], errors="coerce").dropna()
    if s.empty:
        return False
    fig, ax = plt.subplots(figsize=(8, 5))
    ax.hist(s, bins=60, color="steelblue", edgecolor="white", alpha=0.85)
    ax.set_title("Distribuição de tempo (BPM)")
    ax.set_xlabel("tempo")
    ax.set_ylabel("Frequência")
    _fig_save(out)
    return True


def _chart_scatter_energy_danceability(
    df: pd.DataFrame, out: Path, sample_size: int
) -> bool:
    if "energy" not in df.columns or "danceability" not in df.columns:
        return False
    sub = df[["energy", "danceability"]].apply(pd.to_numeric, errors="coerce").dropna()
    if sub.empty:
        return False
    n = min(len(sub), max(1, sample_size))
    sampled = sub.sample(n=n, random_state=42)
    fig, ax = plt.subplots(figsize=(8, 5))
    ax.scatter(
        sampled["danceability"],
        sampled["energy"],
        s=4,
        alpha=0.35,
        c="darkgreen",
    )
    ax.set_title(f"Energy vs danceability (amostra n={n:,})")
    ax.set_xlabel("danceability")
    ax.set_ylabel("energy")
    _fig_save(out)
    return True


def _chart_correlation(df: pd.DataFrame, out: Path) -> bool:
    cols = [c for c in _CORR_CANDIDATES if c in df.columns]
    if len(cols) < 2:
        return False
    num = df[cols].apply(pd.to_numeric, errors="coerce")
    num = num.dropna(how="all")
    if len(num) < 2:
        return False
    corr = num.corr()
    if corr.empty or corr.shape[0] < 2:
        return False
    arr = corr.values.astype(float)
    fig, ax = plt.subplots(figsize=(10, 8))
    im = ax.imshow(arr, cmap="coolwarm", vmin=-1.0, vmax=1.0, aspect="auto")
    labels = list(corr.columns)
    ax.set_xticks(np.arange(len(labels)))
    ax.set_yticks(np.arange(len(labels)))
    ax.set_xticklabels(labels, rotation=45, ha="right")
    ax.set_yticklabels(labels)
    ax.set_title("Correlação entre features numéricas (áudio)")
    fig.colorbar(im, ax=ax, fraction=0.046, pad=0.04)
    _fig_save(out)
    return True


def _chart_decade(df: pd.DataFrame, out: Path) -> bool:
    if "decade" not in df.columns:
        return False
    s = pd.to_numeric(df["decade"], errors="coerce").dropna()
    if s.empty:
        return False
    counts = s.astype(int).value_counts().sort_index()
    fig, ax = plt.subplots(figsize=(10, 5))
    ax.bar(counts.index.astype(str), counts.values, color="coral", edgecolor="white")
    ax.set_title("Quantidade de faixas por década")
    ax.set_xlabel("decade")
    ax.set_ylabel("Contagem")
    plt.setp(ax.xaxis.get_majorticklabels(), rotation=45, ha="right")
    _fig_save(out)
    return True


def _chart_explicit(df: pd.DataFrame, out: Path) -> bool:
    if "explicit" not in df.columns:
        return False
    s = pd.to_numeric(df["explicit"], errors="coerce").dropna()
    if s.empty:
        return False
    counts = s.astype(int).value_counts().sort_index()
    fig, ax = plt.subplots(figsize=(6, 5))
    labels = [str(int(i)) for i in counts.index]
    ax.bar(labels, counts.values, color=["#6baed6", "#fd8d3c"][: len(labels)], edgecolor="white")
    ax.set_title("Distribuição de conteúdo explícito (0 = não, 1 = sim)")
    ax.set_xlabel("explicit")
    ax.set_ylabel("Contagem")
    _fig_save(out)
    return True


def gerar_graficos_e_relatorio(df: pd.DataFrame, settings: Settings) -> Path:
    """
    Gera PNGs em ``<docs_dir>/graficos/`` e o Markdown ``<docs_dir>/graficos.md``.
    """
    docs_base = Path(settings.docs_dir)
    plots_dir = docs_base / "graficos"
    md_path = docs_base / "graficos.md"

    criar_pasta_se_nao_existe(docs_base)
    criar_pasta_se_nao_existe(plots_dir)

    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    sample_size = settings.silver_scatter_sample_size

    sections: list[tuple[str, str, str]] = [
        (
            "1. Histograma — tempo (BPM)",
            "graficos/01_histogram_tempo.png",
            "01_histogram_tempo.png",
        ),
        (
            "2. Dispersão — energy vs danceability (amostra)",
            "graficos/02_scatter_energy_danceability.png",
            "02_scatter_energy_danceability.png",
        ),
        (
            "3. Mapa de calor — correlação entre features",
            "graficos/03_correlacao_features.png",
            "03_correlacao_features.png",
        ),
        (
            "4. Barras — faixas por década",
            "graficos/04_barras_decada.png",
            "04_barras_decada.png",
        ),
        (
            "5. Barras — conteúdo explícito",
            "graficos/05_explicit.png",
            "05_explicit.png",
        ),
    ]

    ok_flags = [
        _chart_histogram_tempo(df, plots_dir / sections[0][2]),
        _chart_scatter_energy_danceability(
            df, plots_dir / sections[1][2], sample_size
        ),
        _chart_correlation(df, plots_dir / sections[2][2]),
        _chart_decade(df, plots_dir / sections[3][2]),
        _chart_explicit(df, plots_dir / sections[4][2]),
    ]

    lines = [
        f"# Gráficos — Silver ({settings.dataset})",
        "",
        f"> Gerado em {timestamp}",
        "",
    ]

    for (titulo, rel_md, _), gerou in zip(sections, ok_flags):
        lines.append(f"## {titulo}")
        lines.append("")
        if gerou:
            lines.append(f"![]({rel_md})")
        else:
            lines.append(
                "*Gráfico omitido: colunas insuficientes ou dados vazios para esta visualização.*"
            )
        lines.append("")

    md_path.write_text("\n".join(lines), encoding="utf-8")
    print(f"[INFO] Markdown de gráficos salvo em: {md_path}")

    return md_path
