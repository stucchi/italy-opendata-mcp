"""Dataset download, caching, and SQLite build logic.

Sources (in priority order):
  - ISTAT Elenco Comuni CSV  (admin)       — official, monthly
  - ANPR popolazione CSV     (population)  — official, daily
  - ISTAT Classificazioni    (geo stats)   — official, annual
  - comuni-json              (CAP only)    — community, no official alternative
  - opendatasicilia          (coordinates) — community, no official alternative
"""

from __future__ import annotations

import asyncio
import csv
import io
import json
import os
import platform
import re
import sqlite3
import zipfile
from datetime import datetime, timezone
from pathlib import Path

import httpx

from italy_opendata_mcp.normalise import normalise

APP_NAME = "italy-opendata-mcp"

# ---------------------------------------------------------------------------
# Data source URLs
# ---------------------------------------------------------------------------

ISTAT_ADMIN_URL = (
    "https://www.istat.it/storage/codici-unita-amministrative/"
    "Elenco-comuni-italiani.csv"
)
ANPR_POP_URL = (
    "https://raw.githubusercontent.com/italia/anpr-opendata/"
    "main/data/popolazione_residente_export.csv"
)
ISTAT_GEO_URL = (
    "https://www.istat.it/wp-content/uploads/2024/05/"
    "Classificazioni-statistiche-Anno_2026.zip"
)
COMUNI_JSON_URL = (
    "https://raw.githubusercontent.com/matteocontrini/comuni-json/"
    "master/comuni.json"
)
COORDS_URL = (
    "https://raw.githubusercontent.com/opendatasicilia/comuni-italiani/"
    "main/dati/coordinate.csv"
)

# ---------------------------------------------------------------------------
# Look-up tables
# ---------------------------------------------------------------------------

ZONE_ALTIMETRICHE = {
    "1": "Montagna interna",
    "2": "Montagna litoranea",
    "3": "Collina interna",
    "4": "Collina litoranea",
    "5": "Pianura",
}

# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------

SCHEMA = """\
CREATE TABLE IF NOT EXISTS regioni (
    codice TEXT PRIMARY KEY,
    nome TEXT NOT NULL,
    nome_normalizzato TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS province (
    codice TEXT PRIMARY KEY,
    nome TEXT NOT NULL,
    nome_normalizzato TEXT NOT NULL,
    sigla TEXT NOT NULL,
    codice_regione TEXT NOT NULL REFERENCES regioni(codice)
);

CREATE TABLE IF NOT EXISTS comuni (
    codice_istat TEXT PRIMARY KEY,
    nome TEXT NOT NULL,
    nome_normalizzato TEXT NOT NULL,
    codice_provincia TEXT NOT NULL REFERENCES province(codice),
    codice_regione TEXT NOT NULL REFERENCES regioni(codice),
    sigla_provincia TEXT NOT NULL,
    codice_catastale TEXT,
    popolazione INTEGER,
    superficie_kmq REAL,
    altitudine INTEGER,
    zona_altimetrica TEXT,
    litoraneo INTEGER DEFAULT 0,
    isolano INTEGER DEFAULT 0,
    grado_urbanizzazione INTEGER,
    latitudine REAL,
    longitudine REAL
);

CREATE TABLE IF NOT EXISTS cap_comuni (
    cap TEXT NOT NULL,
    codice_istat TEXT NOT NULL REFERENCES comuni(codice_istat),
    PRIMARY KEY (cap, codice_istat)
);

CREATE INDEX IF NOT EXISTS idx_cap ON cap_comuni(cap);
CREATE INDEX IF NOT EXISTS idx_comuni_nome ON comuni(nome_normalizzato);
CREATE INDEX IF NOT EXISTS idx_comuni_provincia ON comuni(codice_provincia);
CREATE INDEX IF NOT EXISTS idx_comuni_regione ON comuni(codice_regione);
CREATE INDEX IF NOT EXISTS idx_province_sigla ON province(sigla);
CREATE INDEX IF NOT EXISTS idx_province_nome ON province(nome_normalizzato);
CREATE INDEX IF NOT EXISTS idx_regioni_nome ON regioni(nome_normalizzato);
"""

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------


def cache_dir() -> Path:
    if platform.system() == "Windows":
        base = Path(
            os.environ.get("LOCALAPPDATA", str(Path.home() / "AppData" / "Local"))
        )
    else:
        base = Path(os.environ.get("XDG_CACHE_HOME", str(Path.home() / ".cache")))
    d = base / APP_NAME
    d.mkdir(parents=True, exist_ok=True)
    return d


def db_path() -> Path:
    return cache_dir() / "italia.db"


def _manifest_path() -> Path:
    return cache_dir() / "manifest.json"


def _read_manifest() -> dict:
    p = _manifest_path()
    if p.exists():
        return json.loads(p.read_text())
    return {}


def _write_manifest(data: dict) -> None:
    _manifest_path().write_text(json.dumps(data, indent=2, ensure_ascii=False))


# ---------------------------------------------------------------------------
# ISTAT accent fix  (ISTAT uses  e'  instead of  è)
# ---------------------------------------------------------------------------

_ACCENT_MAP = {
    "a": "à", "e": "è", "i": "ì", "o": "ò", "u": "ù",
    "A": "À", "E": "È", "I": "Ì", "O": "Ò", "U": "Ù",
}


def _fix_accents(name: str) -> str:
    """``Aglie'`` → ``Agliè``, but ``Sant'Angelo`` stays unchanged."""
    return re.sub(
        r"([aeiouAEIOU])'(?=\s|$|[^a-zA-Z])",
        lambda m: _ACCENT_MAP[m.group(1)],
        name,
    )


# ---------------------------------------------------------------------------
# Safe type helpers
# ---------------------------------------------------------------------------


def _safe_int(val: object) -> int | None:
    if val is None:
        return None
    try:
        return int(val)
    except (ValueError, TypeError):
        return None


def _safe_float(val: object) -> float | None:
    if val is None:
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None


# ---------------------------------------------------------------------------
# Download helpers  (each returns a partial dict keyed by ISTAT code)
# ---------------------------------------------------------------------------


async def _download_istat_admin(
    client: httpx.AsyncClient,
) -> dict:
    """ISTAT Elenco Comuni CSV → admin structure.

    Returns ``{"comuni": [...], "regioni": {code: name}, "province": {code: {...}}}``.
    """
    resp = await client.get(ISTAT_ADMIN_URL, timeout=60)
    resp.raise_for_status()
    text = resp.content.decode("cp1252")
    reader = csv.reader(io.StringIO(text), delimiter=";")

    regioni: dict[str, str] = {}
    province: dict[str, dict] = {}
    comuni: list[dict] = []

    for row in reader:
        if len(row) < 20:
            continue
        codice_istat = row[4].strip()
        if not codice_istat or len(codice_istat) != 6:
            continue
        # skip non-numeric (header rows)
        try:
            int(codice_istat)
        except ValueError:
            continue

        cod_reg = row[0].strip().zfill(2)
        nome_reg = _fix_accents(row[10].strip())
        cod_prov = row[2].strip().zfill(3)
        nome_prov = _fix_accents(row[11].strip())
        sigla = row[14].strip()
        nome_comune = _fix_accents(
            row[6].strip() or row[5].strip()  # prefer Italian name
        )
        cod_cat = row[19].strip()

        regioni[cod_reg] = nome_reg

        if cod_prov not in province:
            province[cod_prov] = {
                "nome": nome_prov,
                "sigla": sigla,
                "codice_regione": cod_reg,
            }

        comuni.append(
            {
                "codice_istat": codice_istat,
                "nome": nome_comune,
                "codice_regione": cod_reg,
                "codice_provincia": cod_prov,
                "sigla": sigla,
                "codice_catastale": cod_cat,
            }
        )

    return {"comuni": comuni, "regioni": regioni, "province": province}


async def _download_anpr_pop(
    client: httpx.AsyncClient,
) -> dict[str, int]:
    """ANPR daily population → ``{istat_code: residents}``."""
    try:
        resp = await client.get(ANPR_POP_URL, timeout=60)
        resp.raise_for_status()
    except Exception:
        return {}

    pop: dict[str, int] = {}
    reader = csv.DictReader(io.StringIO(resp.text))
    for row in reader:
        code = (row.get("COD_ISTAT_COMUNE") or "").strip()
        val = (row.get("RESIDENTI") or "").strip()
        if code and val:
            try:
                pop[code] = int(val)
            except ValueError:
                pass
    return pop


async def _download_istat_geo(
    client: httpx.AsyncClient,
) -> dict[str, dict]:
    """ISTAT Classificazioni XLSX (inside ZIP) → geo stats per comune."""
    try:
        resp = await client.get(ISTAT_GEO_URL, timeout=120)
        resp.raise_for_status()
    except Exception:
        return {}

    import openpyxl

    try:
        with zipfile.ZipFile(io.BytesIO(resp.content)) as zf:
            xlsx_names = [n for n in zf.namelist() if n.endswith(".xlsx")]
            if not xlsx_names:
                return {}
            with zf.open(xlsx_names[0]) as f:
                wb = openpyxl.load_workbook(
                    io.BytesIO(f.read()), read_only=True, data_only=True
                )
                ws = wb.active
                rows = list(ws.iter_rows(values_only=True))
                wb.close()
    except Exception:
        return {}

    # Columns (0-indexed):
    #  [1] Codice ISTAT alfanumerico
    #  [5] Superficie km²
    #  [7] Popolazione residente (fallback)
    #  [8] Zona altimetrica (code 1-5)
    #  [9] Altitudine del centro (m)
    # [10] Litoraneo (0/1)
    # [11] Isolano (0/1)
    # [13] Grado di urbanizzazione (1-3)
    geo: dict[str, dict] = {}
    for row in rows:
        if not row or len(row) < 14:
            continue
        raw_code = row[1]
        if raw_code is None:
            continue
        code = str(int(raw_code)) if isinstance(raw_code, (int, float)) else str(raw_code).strip()
        code = code.zfill(6)
        if len(code) != 6 or not code.isdigit():
            continue

        za_raw = row[8]
        za_code = str(int(za_raw)) if isinstance(za_raw, (int, float)) else str(za_raw).strip() if za_raw else None
        zona = ZONE_ALTIMETRICHE.get(za_code, za_code) if za_code else None

        geo[code] = {
            "superficie_kmq": _safe_float(row[5]),
            "popolazione": _safe_int(row[7]),
            "altitudine": _safe_int(row[9]),
            "zona_altimetrica": zona,
            "litoraneo": _safe_int(row[10]),
            "isolano": _safe_int(row[11]),
            "grado_urbanizzazione": _safe_int(row[13]),
        }

    return geo


async def _download_cap(
    client: httpx.AsyncClient,
) -> dict[str, list[str]]:
    """comuni-json → ``{istat_code: [cap, …]}`` (only source for CAP)."""
    try:
        resp = await client.get(COMUNI_JSON_URL, timeout=120)
        resp.raise_for_status()
    except Exception:
        return {}

    return {c["codice"]: c.get("cap", []) for c in resp.json()}


async def _download_coords(
    client: httpx.AsyncClient,
) -> dict[str, tuple[float, float]]:
    """opendatasicilia → ``{istat_code: (lat, lng)}``."""
    try:
        resp = await client.get(COORDS_URL, timeout=60)
        resp.raise_for_status()
    except Exception:
        return {}

    coords: dict[str, tuple[float, float]] = {}
    for line in resp.text.strip().split("\n")[1:]:
        parts = line.split(",")
        if len(parts) == 3:
            code = parts[0].strip().zfill(6)
            try:
                coords[code] = (float(parts[1]), float(parts[2]))
            except ValueError:
                pass
    return coords


# ---------------------------------------------------------------------------
# Download orchestrator
# ---------------------------------------------------------------------------


async def _download_all() -> tuple[dict, dict, dict, dict, dict]:
    async with httpx.AsyncClient(follow_redirects=True) as client:
        admin, pop, geo, cap, coords = await asyncio.gather(
            _download_istat_admin(client),
            _download_anpr_pop(client),
            _download_istat_geo(client),
            _download_cap(client),
            _download_coords(client),
        )
    return admin, pop, geo, cap, coords


# ---------------------------------------------------------------------------
# Build
# ---------------------------------------------------------------------------


def _build_db(
    path: Path,
    admin: dict,
    population: dict[str, int],
    geo: dict[str, dict],
    cap_map: dict[str, list[str]],
    coords: dict[str, tuple[float, float]],
) -> dict:
    """Merge all sources into a single SQLite database. Returns stats."""
    conn = sqlite3.connect(str(path))
    try:
        conn.executescript(SCHEMA)

        # Regioni
        for codice, nome in admin["regioni"].items():
            conn.execute(
                "INSERT OR IGNORE INTO regioni (codice, nome, nome_normalizzato) "
                "VALUES (?, ?, ?)",
                (codice, nome, normalise(nome)),
            )

        # Province
        for codice, info in admin["province"].items():
            conn.execute(
                "INSERT OR IGNORE INTO province "
                "(codice, nome, nome_normalizzato, sigla, codice_regione) "
                "VALUES (?, ?, ?, ?, ?)",
                (
                    codice,
                    info["nome"],
                    normalise(info["nome"]),
                    info["sigla"],
                    info["codice_regione"],
                ),
            )

        # Comuni
        for c in admin["comuni"]:
            istat = c["codice_istat"]

            # Population: ANPR (daily) → ISTAT Classificazioni (annual) fallback
            pop = population.get(istat)
            if pop is None:
                pop = (geo.get(istat) or {}).get("popolazione")

            g = geo.get(istat, {})
            lat, lng = coords.get(istat, (None, None))

            conn.execute(
                """INSERT OR IGNORE INTO comuni
                   (codice_istat, nome, nome_normalizzato, codice_provincia,
                    codice_regione, sigla_provincia, codice_catastale,
                    popolazione, superficie_kmq, altitudine,
                    zona_altimetrica, litoraneo, isolano,
                    grado_urbanizzazione, latitudine, longitudine)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    istat,
                    c["nome"],
                    normalise(c["nome"]),
                    c["codice_provincia"],
                    c["codice_regione"],
                    c["sigla"],
                    c["codice_catastale"],
                    pop,
                    g.get("superficie_kmq"),
                    g.get("altitudine"),
                    g.get("zona_altimetrica"),
                    g.get("litoraneo"),
                    g.get("isolano"),
                    g.get("grado_urbanizzazione"),
                    lat,
                    lng,
                ),
            )

            for cap_val in cap_map.get(istat, []):
                conn.execute(
                    "INSERT OR IGNORE INTO cap_comuni (cap, codice_istat) "
                    "VALUES (?, ?)",
                    (cap_val, istat),
                )

        conn.commit()

        stats = {
            "comuni": conn.execute("SELECT COUNT(*) FROM comuni").fetchone()[0],
            "regioni": conn.execute("SELECT COUNT(*) FROM regioni").fetchone()[0],
            "province": conn.execute("SELECT COUNT(*) FROM province").fetchone()[0],
            "con_popolazione": conn.execute(
                "SELECT COUNT(*) FROM comuni WHERE popolazione IS NOT NULL"
            ).fetchone()[0],
            "con_coordinate": conn.execute(
                "SELECT COUNT(*) FROM comuni WHERE latitudine IS NOT NULL"
            ).fetchone()[0],
            "con_cap": conn.execute(
                "SELECT COUNT(DISTINCT codice_istat) FROM cap_comuni"
            ).fetchone()[0],
            "con_geo": conn.execute(
                "SELECT COUNT(*) FROM comuni WHERE superficie_kmq IS NOT NULL"
            ).fetchone()[0],
        }
        return stats
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


async def ensure_db() -> Path:
    """Return path to the local SQLite DB, downloading on first use."""
    p = db_path()
    if p.exists():
        return p

    admin, pop, geo, cap, coords = await _download_all()
    stats = _build_db(p, admin, pop, geo, cap, coords)

    _write_manifest(
        {
            "sources": {
                "istat_admin": ISTAT_ADMIN_URL,
                "anpr_population": ANPR_POP_URL,
                "istat_geo": ISTAT_GEO_URL,
                "comuni_json_cap": COMUNI_JSON_URL,
                "coordinates": COORDS_URL,
            },
            "downloaded_at": datetime.now(timezone.utc).isoformat(),
            "records": stats,
        }
    )
    return p


async def refresh(force: bool = False) -> dict:
    """Re-download and rebuild the database."""
    p = db_path()
    if p.exists() and not force:
        return {"status": "already_exists", "path": str(p), **dataset_status()}
    if p.exists():
        p.unlink()

    admin, pop, geo, cap, coords = await _download_all()
    stats = _build_db(p, admin, pop, geo, cap, coords)

    manifest = {
        "sources": {
            "istat_admin": ISTAT_ADMIN_URL,
            "anpr_population": ANPR_POP_URL,
            "istat_geo": ISTAT_GEO_URL,
            "comuni_json_cap": COMUNI_JSON_URL,
            "coordinates": COORDS_URL,
        },
        "downloaded_at": datetime.now(timezone.utc).isoformat(),
        "records": stats,
    }
    _write_manifest(manifest)
    return {"status": "refreshed", **manifest}


def dataset_status() -> dict:
    """Return current cache status."""
    p = db_path()
    manifest = _read_manifest()
    if not p.exists():
        return {"status": "not_downloaded", "path": str(p)}
    stat = p.stat()
    return {
        "status": "ready",
        "path": str(p),
        "size_mb": round(stat.st_size / (1024 * 1024), 2),
        "downloaded_at": manifest.get("downloaded_at", "unknown"),
        "records": manifest.get("records", {}),
        "sources": manifest.get("sources", {}),
    }
