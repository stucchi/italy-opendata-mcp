"""MCP server – Italian open-data tools."""

from __future__ import annotations

import json
import sqlite3
from typing import Annotated

from mcp.server.fastmcp import FastMCP

from italy_opendata_mcp.cache import (
    dataset_status,
    ensure_db,
    refresh,
)
from italy_opendata_mcp.normalise import normalise

mcp = FastMCP("italy-opendata-mcp")

# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

_conn: sqlite3.Connection | None = None


def _format(data: object) -> str:
    return json.dumps(data, indent=2, ensure_ascii=False, default=str)


async def _ensure_ready() -> None:
    global _conn
    if _conn is not None:
        return
    path = await ensure_db()
    _conn = sqlite3.connect(str(path))
    _conn.row_factory = sqlite3.Row


def _query(sql: str, params: tuple = ()) -> list[dict]:
    assert _conn is not None
    cur = _conn.execute(sql, params)
    return [dict(row) for row in cur.fetchall()]


def _query_one(sql: str, params: tuple = ()) -> dict | None:
    rows = _query(sql, params)
    return rows[0] if rows else None


# ---------------------------------------------------------------------------
# Common SELECT fragment
# ---------------------------------------------------------------------------

_COMUNE_COLS = """\
    c.codice_istat,
    c.nome,
    c.codice_catastale,
    c.popolazione,
    c.superficie_kmq,
    c.altitudine,
    c.zona_altimetrica,
    c.litoraneo,
    c.isolano,
    c.grado_urbanizzazione,
    c.latitudine,
    c.longitudine,
    c.sigla_provincia,
    p.nome   AS provincia,
    p.codice AS codice_provincia,
    r.nome   AS regione,
    r.codice AS codice_regione\
"""

_COMUNE_FROM = """\
FROM comuni c
JOIN province p ON c.codice_provincia = p.codice
JOIN regioni  r ON c.codice_regione   = r.codice\
"""


def _attach_caps(rows: list[dict]) -> None:
    """Add the list of CAP codes to each comune dict, in-place."""
    for row in rows:
        caps = _query(
            "SELECT cap FROM cap_comuni WHERE codice_istat = ? ORDER BY cap",
            (row["codice_istat"],),
        )
        row["cap"] = [c["cap"] for c in caps]


# ---------------------------------------------------------------------------
# Tools
# ---------------------------------------------------------------------------


@mcp.tool()
async def list_regioni() -> str:
    """Elenca tutte le regioni italiane con conteggio comuni e popolazione totale."""
    await _ensure_ready()
    rows = _query(
        """SELECT r.codice, r.nome,
                  COUNT(c.codice_istat) AS num_comuni,
                  SUM(c.popolazione) AS popolazione_totale
           FROM regioni r
           LEFT JOIN comuni c ON c.codice_regione = r.codice
           GROUP BY r.codice, r.nome
           ORDER BY r.nome"""
    )
    return _format({"count": len(rows), "regioni": rows})


@mcp.tool()
async def list_province(
    regione: Annotated[str | None, "Filtra per regione (nome o codice)"] = None,
) -> str:
    """Elenca le province italiane con conteggio comuni e popolazione totale."""
    await _ensure_ready()

    where = ""
    params: tuple = ()
    if regione:
        norm_r = normalise(regione)
        where = "WHERE r.codice = ? OR r.nome_normalizzato = ? OR r.nome_normalizzato LIKE ?"
        params = (regione.strip(), norm_r, f"%{norm_r}%")

    rows = _query(
        f"""SELECT p.codice, p.nome, p.sigla, r.nome AS regione,
                   COUNT(c.codice_istat) AS num_comuni,
                   SUM(c.popolazione) AS popolazione_totale
            FROM province p
            JOIN regioni r ON p.codice_regione = r.codice
            LEFT JOIN comuni c ON c.codice_provincia = p.codice
            {where}
            GROUP BY p.codice, p.nome, p.sigla, r.nome
            ORDER BY r.nome, p.nome""",
        params,
    )
    return _format({"count": len(rows), "province": rows})


@mcp.tool()
async def get_by_cap(
    cap: Annotated[str, "Codice di avviamento postale (es. '00118')"],
) -> str:
    """Trova i comuni associati a un determinato CAP."""
    await _ensure_ready()
    rows = _query(
        f"SELECT {_COMUNE_COLS} {_COMUNE_FROM} "
        "JOIN cap_comuni cc ON c.codice_istat = cc.codice_istat "
        "WHERE cc.cap = ?",
        (cap.strip(),),
    )
    if not rows:
        return _format({"error": f"Nessun comune trovato per il CAP {cap}"})
    _attach_caps(rows)
    return _format(rows if len(rows) > 1 else rows[0])


@mcp.tool()
async def get_comune(
    nome_o_codice: Annotated[
        str,
        "Nome del comune o codice ISTAT a 6 cifre (es. 'Roma' o '058091')",
    ],
) -> str:
    """Restituisce i dettagli di un comune cercato per nome o codice ISTAT."""
    await _ensure_ready()
    v = nome_o_codice.strip()

    # Try exact ISTAT code
    if v.isdigit():
        row = _query_one(
            f"SELECT {_COMUNE_COLS} {_COMUNE_FROM} WHERE c.codice_istat = ?", (v,)
        )
        if row:
            _attach_caps([row])
            return _format(row)

    # Try exact normalised name
    norm = normalise(v)
    rows = _query(
        f"SELECT {_COMUNE_COLS} {_COMUNE_FROM} WHERE c.nome_normalizzato = ?",
        (norm,),
    )

    # Fall back to LIKE
    if not rows:
        rows = _query(
            f"SELECT {_COMUNE_COLS} {_COMUNE_FROM} WHERE c.nome_normalizzato LIKE ?",
            (f"%{norm}%",),
        )

    if not rows:
        return _format({"error": f"Nessun comune trovato per '{nome_o_codice}'"})

    _attach_caps(rows)
    return _format(rows[0] if len(rows) == 1 else rows)


@mcp.tool()
async def list_comuni(
    regione: Annotated[str | None, "Filtra per regione (nome o codice)"] = None,
    provincia: Annotated[
        str | None, "Filtra per provincia (nome, sigla o codice)"
    ] = None,
    limit: Annotated[int, "Numero massimo di risultati (default 400)"] = 400,
) -> str:
    """Elenca i comuni italiani con filtri opzionali per regione o provincia."""
    await _ensure_ready()

    where_parts: list[str] = []
    params: list[str | int] = []

    if regione:
        norm_r = normalise(regione)
        where_parts.append(
            "(r.codice = ? OR r.nome_normalizzato = ? OR r.nome_normalizzato LIKE ?)"
        )
        params.extend([regione.strip(), norm_r, f"%{norm_r}%"])

    if provincia:
        norm_p = normalise(provincia)
        where_parts.append(
            "(p.codice = ? OR p.sigla = ? OR p.nome_normalizzato = ? OR p.nome_normalizzato LIKE ?)"
        )
        params.extend([provincia.strip().upper(), provincia.strip().upper(), norm_p, f"%{norm_p}%"])

    where = "WHERE " + " AND ".join(where_parts) if where_parts else ""

    rows = _query(
        f"""SELECT c.codice_istat, c.nome, c.sigla_provincia,
                   p.nome AS provincia, r.nome AS regione, c.popolazione
            {_COMUNE_FROM}
            {where}
            ORDER BY c.nome
            LIMIT ?""",
        tuple([*params, limit]),
    )

    return _format({"count": len(rows), "comuni": rows})


@mcp.tool()
async def refresh_dataset(
    force: Annotated[
        bool, "Forza il re-download anche se i dati sono già presenti"
    ] = False,
) -> str:
    """Aggiorna il dataset scaricando i dati più recenti."""
    global _conn
    if _conn is not None:
        _conn.close()
        _conn = None
    result = await refresh(force=force)
    return _format(result)


@mcp.tool()
async def datasets_status() -> str:
    """Mostra lo stato dei dataset locali (versione, ultimo aggiornamento, dimensione)."""
    return _format(dataset_status())


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def main():
    mcp.run(transport="stdio")
