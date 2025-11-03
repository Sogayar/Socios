# Projeto: Pipeline CNPJ (v0)
# Entregáveis desta versão
# - config.py: configurações, range de meses e utilitários de datas/URLs
# - download.py: coleta de links + download robusto (retries, hash, validação ZIP, manifesto)
# - main.py: CLI (Typer) com comandos para baixar (intervalo completo ou mês específico)
# - extract.py (stub) e merge.py (stub): esqueleto para próxima etapa (extração + consolidação em Parquet/SQLite)
# - requirements.txt sugerido ao final

# ────────────────────────────────────────────────────────────────────────────────
# src/config.py
# ────────────────────────────────────────────────────────────────────────────────
from __future__ import annotations
from pathlib import Path
import datetime as dt

# Diretórios-base
PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = PROJECT_ROOT / "data"
RAW_DIR = DATA_DIR / "raw"
EXTRACTED_DIR = DATA_DIR / "extracted"
PROCESSED_DIR = DATA_DIR / "processed"
DB_DIR = DATA_DIR / "database"
LOGS_DIR = PROJECT_ROOT / "logs"
CHECKSUMS_DIR = DATA_DIR / "checksums"

# Garantir criação lazy (os módulos chamadores é que criam com mkdir)

# Fonte oficial (portal de arquivos da RFB)
BASE_URL = "https://arquivos.receitafederal.gov.br/dados/cnpj/dados_abertos_cnpj"

# Intervalo de meses inclusivo (YYYY, MM)
START_YM = (2024, 8)
END_YM = (2025, 10)

# Filtro de arquivos de interesse (case-insensitive)
KEYWORD_SOCIOS = "socios"  # manter sem acento; os nomes oficiais usam 'Socios'

# HTTP / rede
USER_AGENT = (
    "Mozilla/5.0 (compatible; SA-Advogados-CNPJ-Downloader/1.0; +https://sainfra.com.br)"
)
HTTP_TIMEOUT_S = 60
RETRY_ATTEMPTS = 4
RETRY_MIN_WAIT = 1
RETRY_MAX_WAIT = 20
POLITE_DELAY_S = 0.5  # intervalo curto entre downloads

# Downloads
CHUNK_SIZE = 1024 * 1024  # 1 MiB
CONCURRENT_DOWNLOADS = 1  # manter 1 por cortesia ao servidor

# Manifesto
MANIFEST_PATH = DATA_DIR / "manifest.csv"


def month_iter(start: tuple[int, int] = START_YM, end: tuple[int, int] = END_YM):
    """Gera strings 'YYYY-MM' do intervalo inclusivo."""
    y, m = start
    ye, me = end
    d = dt.date(y, m, 1)
    endd = dt.date(ye, me, 1)
    while d <= endd:
        yield f"{d.year:04d}-{d.month:02d}"
        # avança 1 mês
        m = d.month + 1
        y = d.year + (1 if m == 13 else 0)
        m = 1 if m == 13 else m
        d = dt.date(y, m, 1)


def build_month_url(yyyy_mm: str) -> str:
    return f"{BASE_URL}/{yyyy_mm}/"


def month_to_paths(yyyy_mm: str):
    """Retorna (raw_month_dir, extracted_month_dir, processed_month_dir)."""
    raw = RAW_DIR / yyyy_mm.replace("-", "_")
    ext = EXTRACTED_DIR / yyyy_mm.replace("-", "_")
    proc = PROCESSED_DIR / yyyy_mm.replace("-", "_")
    return raw, ext, proc