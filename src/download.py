# ────────────────────────────────────────────────────────────────────────────────
# src/download.py
# ────────────────────────────────────────────────────────────────────────────────
from __future__ import annotations
import re
import csv
import time
import hashlib
import zipfile
from pathlib import Path
from typing import List, Tuple, Optional

import requests
from bs4 import BeautifulSoup
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from tqdm import tqdm

from .config import (
    RAW_DIR,
    MANIFEST_PATH,
    USER_AGENT,
    HTTP_TIMEOUT_S,
    RETRY_ATTEMPTS,
    RETRY_MIN_WAIT,
    RETRY_MAX_WAIT,
    POLITE_DELAY_S,
    CHUNK_SIZE,
    KEYWORD_SOCIOS,
    build_month_url,
    month_to_paths,
)


# ── utilidades internas ────────────────────────────────────────────────────────

def _ensure_dir(p: Path):
    p.mkdir(parents=True, exist_ok=True)


def _is_socios_zip(name: str) -> bool:
    n = name.lower()
    return (KEYWORD_SOCIOS in n) and n.endswith(".zip")


@retry(
    reraise=True,
    stop=stop_after_attempt(RETRY_ATTEMPTS),
    wait=wait_exponential(multiplier=1, min=RETRY_MIN_WAIT, max=RETRY_MAX_WAIT),
    retry=retry_if_exception_type((requests.RequestException,)),
)
def _fetch_index(url: str) -> str:
    r = requests.get(url, headers={"User-Agent": USER_AGENT}, timeout=HTTP_TIMEOUT_S)
    r.raise_for_status()
    return r.text


def _parse_size_guess(text: str) -> Optional[int]:
    m = re.search(r"(\d[\d\s\.,]*)(?:\s*(B|KB|MB|GB))", text, re.I)
    if not m:
        return None
    num = m.group(1).replace(".", "").replace(" ", "").replace(",", ".")
    unit = (m.group(2) or "B").upper()
    try:
        val = float(num)
        factor = {"B": 1, "KB": 1024, "MB": 1024 ** 2, "GB": 1024 ** 3}.get(unit, 1)
        return int(val * factor)
    except Exception:
        return None


def list_socios_zips(month_url: str) -> List[Tuple[str, Optional[int]]]:
    html = _fetch_index(month_url)
    soup = BeautifulSoup(html, "html.parser")
    out: List[Tuple[str, Optional[int]]] = []
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if not href.lower().endswith(".zip"):
            continue
        name = href.split("/")[-1]
        if not _is_socios_zip(name):
            continue
        abs_url = href if href.startswith("http") else month_url + href
        size_guess = _parse_size_guess(a.parent.get_text(" ", strip=True) if a.parent else "")
        out.append((abs_url, size_guess))
    return out


def _sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _validate_zip(path: Path) -> bool:
    try:
        with zipfile.ZipFile(path, "r") as zf:
            return zf.testzip() is None
    except zipfile.BadZipFile:
        return False


@retry(
    reraise=True,
    stop=stop_after_attempt(RETRY_ATTEMPTS),
    wait=wait_exponential(multiplier=1, min=RETRY_MIN_WAIT, max=RETRY_MAX_WAIT),
    retry=retry_if_exception_type((requests.RequestException,)),
)
def _download(url: str, dest: Path, expected_size: Optional[int] = None):
    tmp = dest.with_suffix(dest.suffix + ".part")
    with requests.get(url, stream=True, timeout=HTTP_TIMEOUT_S, headers={"User-Agent": USER_AGENT}) as r:
        r.raise_for_status()
        total = int(r.headers.get("Content-Length", 0)) or (expected_size or 0)
        with open(tmp, "wb") as f, tqdm(
            total=total if total > 0 else None,
            unit="B",
            unit_scale=True,
            desc=dest.name,
        ) as pbar:
            for chunk in r.iter_content(chunk_size=CHUNK_SIZE):
                if not chunk:
                    continue
                f.write(chunk)
                pbar.update(len(chunk))
    # verificação de tamanho mínimo se houver expectativa
    if expected_size and tmp.stat().st_size < expected_size * 0.9:
        tmp.unlink(missing_ok=True)
        raise IOError("Download menor que 90% do esperado")
    tmp.rename(dest)


def _append_manifest(row: dict):
    header = [
        "timestamp_iso",
        "yyyy_mm",
        "file_name",
        "url",
        "size_bytes",
        "sha256",
        "zip_ok",
        "status",
        "message",
    ]
    newfile = not MANIFEST_PATH.exists()
    MANIFEST_PATH.parent.mkdir(parents=True, exist_ok=True)
    with MANIFEST_PATH.open("a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=header)
        if newfile:
            w.writeheader()
        w.writerow(row)


# ── API pública do módulo ──────────────────────────────────────────────────────

def baixar_mes(yyyy_mm: str) -> None:
    """Baixa todos os ZIPs de 'Sócios' de um mês específico."""
    month_url = build_month_url(yyyy_mm)
    raw_dir, _, _ = month_to_paths(yyyy_mm)
    _ensure_dir(raw_dir)

    try:
        files = list_socios_zips(month_url)
    except Exception as e:
        print(f"[ERRO] Falha ao listar {month_url}: {e}")
        return

    if not files:
        print(f"[INFO] Nenhum ZIP de Sócios encontrado em {yyyy_mm}.")
        return

    for url, size_guess in files:
        fname = url.split("/")[-1]
        dest = raw_dir / fname
        row_base = {
            "timestamp_iso": __import__("datetime").datetime.now().isoformat(timespec="seconds"),
            "yyyy_mm": yyyy_mm,
            "file_name": fname,
            "url": url,
            "size_bytes": "",
            "sha256": "",
            "zip_ok": "",
            "status": "",
            "message": "",
        }

        # já existe e é válido? pula
        if dest.exists():
            try:
                h = _sha256_file(dest)
                ok = _validate_zip(dest)
                if ok:
                    _append_manifest({**row_base, "size_bytes": dest.stat().st_size, "sha256": h, "zip_ok": True, "status": "skipped_exists_ok", "message": "Arquivo já presente e válido."})
                    print(f"[OK] {fname} já existe e é válido.")
                    continue
                else:
                    print(f"[WARN] {fname} existente inválido; rebaixando.")
            except Exception as e:
                print(f"[WARN] Falha ao validar existente ({fname}): {e}; rebaixando.")

        try:
            _download(url, dest, expected_size=size_guess)
            size = dest.stat().st_size
            ok = _validate_zip(dest)
            h = _sha256_file(dest)
            status = "downloaded_ok" if ok else "downloaded_badzip"
            msg = "" if ok else "Zip inválido após download."
            _append_manifest({**row_base, "size_bytes": size, "sha256": h, "zip_ok": ok, "status": status, "message": msg})
            print(f"[{status}] {fname} ({size} bytes)")
        except Exception as e:
            _append_manifest({**row_base, "status": "error", "message": str(e)})
            print(f"[ERRO] {fname}: {e}")
        finally:
            time.sleep(POLITE_DELAY_S)