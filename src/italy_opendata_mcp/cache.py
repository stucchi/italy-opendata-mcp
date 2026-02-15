"""Dataset download, caching, and SQLite build logic."""

from __future__ import annotations

import json
import os
import platform
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

import httpx

from italy_opendata_mcp.normalise import normalise

APP_NAME = "italy-opendata-mcp"

SOURCE_URL = (
    "https://raw.githubusercontent.com/matteocontrini/comuni-json/master/comuni.json"
)
COORDS_URL = (
    "https://raw.githubusercontent.com/opendatasicilia/comuni-italiani/main/dati/coordinate.csv"
)

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
# Download
# ---------------------------------------------------------------------------


async def _download() -> tuple[list[dict], dict[str, tuple[float, float]]]:
    """Download comuni JSON and coordinates CSV. Returns (comuni, coords_map)."""
    async with httpx.AsyncClient(follow_redirects=True) as client:
        resp = await client.get(SOURCE_URL, timeout=120)
        resp.raise_for_status()
        comuni = resp.json()

        coords: dict[str, tuple[float, float]] = {}
        resp_c = await client.get(COORDS_URL, timeout=60)
        if resp_c.status_code == 200:
            for line in resp_c.text.strip().split("\n")[1:]:
                parts = line.split(",")
                if len(parts) == 3:
                    istat = parts[0].strip().zfill(6)
                    coords[istat] = (float(parts[1]), float(parts[2]))

        return comuni, coords


# ---------------------------------------------------------------------------
# Build
# ---------------------------------------------------------------------------


def _build_db(
    path: Path,
    comuni_data: list[dict],
    coords: dict[str, tuple[float, float]] | None = None,
) -> dict:
    """Build the SQLite database from comuni.json + coordinates. Returns stats."""
    coords = coords or {}
    conn = sqlite3.connect(str(path))
    try:
        conn.executescript(SCHEMA)

        regioni_seen: set[str] = set()
        province_seen: set[str] = set()

        for c in comuni_data:
            reg = c["regione"]
            if reg["codice"] not in regioni_seen:
                conn.execute(
                    "INSERT OR IGNORE INTO regioni (codice, nome, nome_normalizzato) VALUES (?, ?, ?)",
                    (reg["codice"], reg["nome"], normalise(reg["nome"])),
                )
                regioni_seen.add(reg["codice"])

            # provincia or città metropolitana
            prov = c.get("provincia", {}) or {}
            cm = c.get("cm", {}) or {}
            prov_codice = prov.get("codice") or cm.get("codice", "")
            prov_nome = prov.get("nome") or cm.get("nome", "")
            sigla = c.get("sigla", "")

            if prov_codice and prov_codice not in province_seen:
                conn.execute(
                    "INSERT OR IGNORE INTO province (codice, nome, nome_normalizzato, sigla, codice_regione) VALUES (?, ?, ?, ?, ?)",
                    (prov_codice, prov_nome, normalise(prov_nome), sigla, reg["codice"]),
                )
                province_seen.add(prov_codice)

            lat, lng = coords.get(c["codice"], (None, None))
            conn.execute(
                """INSERT OR IGNORE INTO comuni
                   (codice_istat, nome, nome_normalizzato, codice_provincia,
                    codice_regione, sigla_provincia, codice_catastale,
                    popolazione, latitudine, longitudine)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    c["codice"],
                    c["nome"],
                    normalise(c["nome"]),
                    prov_codice,
                    reg["codice"],
                    sigla,
                    c.get("codiceCatastale", ""),
                    c.get("popolazione"),
                    lat,
                    lng,
                ),
            )

            for cap_val in c.get("cap", []):
                conn.execute(
                    "INSERT OR IGNORE INTO cap_comuni (cap, codice_istat) VALUES (?, ?)",
                    (cap_val, c["codice"]),
                )

        conn.commit()

        count = conn.execute("SELECT COUNT(*) FROM comuni").fetchone()[0]
        return {
            "comuni": count,
            "regioni": len(regioni_seen),
            "province": len(province_seen),
        }
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
    data, coords = await _download()
    stats = _build_db(p, data, coords)
    _write_manifest(
        {
            "admin": {
                "source_url": SOURCE_URL,
                "downloaded_at": datetime.now(timezone.utc).isoformat(),
                "records": stats,
            }
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

    data, coords = await _download()
    stats = _build_db(p, data, coords)

    manifest = {
        "admin": {
            "source_url": SOURCE_URL,
            "downloaded_at": datetime.now(timezone.utc).isoformat(),
            "records": stats,
        }
    }
    _write_manifest(manifest)
    return {"status": "refreshed", **manifest["admin"]}


def dataset_status() -> dict:
    """Return current cache status."""
    p = db_path()
    manifest = _read_manifest()
    if not p.exists():
        return {"status": "not_downloaded", "path": str(p)}
    info = manifest.get("admin", {})
    stat = p.stat()
    return {
        "status": "ready",
        "path": str(p),
        "size_mb": round(stat.st_size / (1024 * 1024), 2),
        "downloaded_at": info.get("downloaded_at", "unknown"),
        "records": info.get("records", {}),
    }
