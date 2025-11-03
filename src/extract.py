# ────────────────────────────────────────────────────────────────────────────────
# src/extract.py (stub)
# ────────────────────────────────────────────────────────────────────────────────
from __future__ import annotations
import zipfile
from pathlib import Path
from typing import Iterable

from .config import month_to_paths


def extrair_mes(yyyy_mm: str) -> None:
    """Extrai todos os ZIPs do mês para data/extracted/YYYY_MM/ (stub)."""
    raw_dir, extracted_dir, _ = month_to_paths(yyyy_mm)
    extracted_dir.mkdir(parents=True, exist_ok=True)

    zips = sorted(raw_dir.glob("*.zip"))
    if not zips:
        print(f"[INFO] Nenhum ZIP para extrair em {yyyy_mm} (dir: {raw_dir}).")
        return

    for z in zips:
        try:
            with zipfile.ZipFile(z, "r") as zf:
                zf.extractall(extracted_dir)
            print(f"[OK] Extraído: {z.name}")
        except zipfile.BadZipFile:
            print(f"[ERRO] ZIP inválido: {z}")