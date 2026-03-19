from __future__ import annotations

import os
import zipfile


def descompactar_zip(destino: str) -> None:
    zip_files = [f for f in os.listdir(destino) if f.endswith(".zip")]

    if not zip_files:
        print("[WARN] Nenhum arquivo .zip encontrado para descompactar.")
        return

    for zip_name in zip_files:
        zip_path = os.path.join(destino, zip_name)
        print(f"[INFO] Descompactando {zip_name}...")
        with zipfile.ZipFile(zip_path, "r") as zf:
            zf.extractall(destino)
        os.remove(zip_path)  # remove o .zip após extrair
        print(f"[INFO] {zip_name} removido após extração.")

