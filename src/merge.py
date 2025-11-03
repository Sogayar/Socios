# ────────────────────────────────────────────────────────────────────────────────
# src/merge.py (stub)
# ────────────────────────────────────────────────────────────────────────────────
from __future__ import annotations
from pathlib import Path
from typing import List

# Na próxima etapa, usaremos Polars para ler CSVs sem cabeçalho, aplicar schema
# e salvar Parquet. Aqui deixamos o esqueleto para manter o encadeamento.

from .config import month_to_paths

# Exemplo de schema (placeholders) — substituir pelos campos reais dos "Sócios"
SOCIOS_COLUMNS = [
    "cnpj_basico",
    "identificador_socio",
    "nome_socio",
    "cnpj_cpf_do_socio",
    "qualificacao_socio",
    "data_entrada_sociedade",
    "pais",
    "cpf_representante_legal",
    "nome_representante_legal",
    "qualificacao_representante_legal",
    "faixa_etaria",
]

def consolidar_mes_para_parquet(yyyy_mm: str) -> Path:
    """(stub) Concatena CSVs extraídos do mês e salva Parquet em data/processed/.

    Retorna o caminho do arquivo Parquet gerado.
    """
    _, extracted_dir, processed_dir = month_to_paths(yyyy_mm)
    processed_dir.mkdir(parents=True, exist_ok=True)

    # Placeholder — próxima etapa implementa com Polars em streaming.
    parquet_path = processed_dir / f"socios_{yyyy_mm.replace('-', '_')}.parquet"
    print(f"[TODO] Implementar leitura CSV->schema->Parquet. Alvo: {parquet_path}")
    return parquet_path