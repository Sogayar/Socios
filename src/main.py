# ────────────────────────────────────────────────────────────────────────────────
# src/main.py (CLI)
# ────────────────────────────────────────────────────────────────────────────────
from __future__ import annotations
import typer
from typing import Optional

from .config import month_iter, START_YM, END_YM
from .download import baixar_mes
from .extract import extrair_mes
from .merge import consolidar_mes_para_parquet

app = typer.Typer(add_help_option=True)


@app.command()
def baixar(
    inicio: Optional[str] = typer.Argument(None, help="Mês inicial no formato YYYY-MM. Ex.: 2024-08"),
    fim: Optional[str] = typer.Argument(None, help="Mês final no formato YYYY-MM. Ex.: 2025-10"),
):
    """Baixa os arquivos de 'Sócios' para o(s) mês(es) indicado(s)."""
    if inicio and fim:
        months = _range_from_strings(inicio, fim)
    elif inicio and not fim:
        months = [inicio]
    else:
        # padrão: intervalo completo do projeto
        months = list(month_iter(START_YM, END_YM))

    for ym in months:
        typer.echo(f"\n=== Baixando {ym} ===")
        baixar_mes(ym)


@app.command()
def extrair(
    mes: Optional[str] = typer.Argument(None, help="Mês (YYYY-MM). Se omitido, roda para todo o intervalo."),
):
    """Extrai todos os ZIPs do mês (ou de todo o intervalo)."""
    months = [mes] if mes else list(month_iter(START_YM, END_YM))
    for ym in months:
        typer.echo(f"\n=== Extraindo {ym} ===")
        extrair_mes(ym)


@app.command()
def consolidar(
    mes: Optional[str] = typer.Argument(None, help="Mês (YYYY-MM). Se omitido, roda para todo o intervalo."),
):
    """(stub) Consolida CSVs extraídos do mês em Parquet (etapa v1)."""
    months = [mes] if mes else list(month_iter(START_YM, END_YM))
    for ym in months:
        typer.echo(f"\n=== Consolidando {ym} ===")
        consolidar_mes_para_parquet(ym)


@app.command()
def tudo():
    """Pipeline completo (baixar -> extrair -> consolidar) para todo o intervalo padrão."""
    for ym in month_iter(START_YM, END_YM):
        typer.echo(f"\n=== {ym} :: BAIXAR ===")
        baixar_mes(ym)
        typer.echo(f"=== {ym} :: EXTRAIR ===")
        extrair_mes(ym)
        typer.echo(f"=== {ym} :: CONSOLIDAR ===")
        consolidar_mes_para_parquet(ym)


# ── helpers ────────────────────────────────────────────────────────────────────

def _range_from_strings(inicio: str, fim: str):
    y1, m1 = map(int, inicio.split("-"))
    y2, m2 = map(int, fim.split("-"))
    import datetime as dt

    d = dt.date(y1, m1, 1)
    endd = dt.date(y2, m2, 1)
    out = []
    while d <= endd:
        out.append(f"{d.year:04d}-{d.month:02d}")
        m1 = d.month + 1
        y1 = d.year + (1 if m1 == 13 else 0)
        m1 = 1 if m1 == 13 else m1
        d = dt.date(y1, m1, 1)
    return out

if __name__ == "__main__":
    app()