# italy-opendata-mcp

MCP server exposing Italian open data (municipalities, provinces, regions, postal codes, coordinates, geographic data) through simple, developer-friendly tools.

## Features

- **7 MCP tools** to navigate the Italian administrative hierarchy
- **Official sources**: ISTAT and ANPR where available
- **Lazy download**: data is fetched on first use and cached locally (~1.8 MB SQLite)
- **Offline after first use**: all queries are local
- **No Docker**: installable via `uvx` or `pip`, starts and stops with Claude

## Data sources

| Data | Source | Type |
|------|--------|------|
| Municipalities, provinces, regions, ISTAT codes | [ISTAT](https://www.istat.it/classificazione/codici-dei-comuni-delle-province-e-delle-regioni/) | Official |
| Resident population | [ANPR](https://github.com/italia/anpr-opendata) | Official (daily updates) |
| Surface area, altitude, altimetric zone | [ISTAT](https://www.istat.it/classificazione/principali-statistiche-geografiche-sui-comuni/) | Official |
| Postal codes (CAP) | [comuni-json](https://github.com/matteocontrini/comuni-json) | Community (no official source available) |
| Centroid coordinates | [opendatasicilia](https://github.com/opendatasicilia/comuni-italiani) | Community (no official source available) |

## Installation

```bash
uvx italy-opendata-mcp
```

## Usage in .mcp.json

```json
{
  "mcpServers": {
    "italy-opendata": {
      "command": "uvx",
      "args": ["italy-opendata-mcp"]
    }
  }
}
```

### From source

```bash
git clone https://github.com/stucchi/italy-opendata-mcp.git
cd italy-opendata-mcp
uv venv && uv pip install -e .
```

## Tools

### Hierarchical navigation

```
list_regioni()  →  list_province(regione="Lombardia")  →  list_comuni(provincia="MI")
```

| Tool | Parameters | Description |
|------|------------|-------------|
| `list_regioni` | — | All 20 regions with municipality count and population |
| `list_province` | `regione?` | Provinces with optional region filter |
| `list_comuni` | `regione?`, `provincia?`, `limit?` | Municipalities with optional filters (default 400 results) |

### Search

| Tool | Parameters | Description |
|------|------------|-------------|
| `get_comune` | `nome_o_codice` | Full details of a municipality by name or ISTAT code |
| `get_by_cap` | `cap` | Find municipalities associated with a postal code |

### Data management

| Tool | Parameters | Description |
|------|------------|-------------|
| `refresh_dataset` | `force?` | Re-download data from sources |
| `datasets_status` | — | Local cache status |

## Available fields per municipality

Each municipality includes:

- **Registry**: name, ISTAT code, cadastral code, province abbreviation, province, region
- **Demographics**: population (ANPR, daily updates)
- **Geography**: latitude, longitude, surface area (km²), altitude (m), altimetric zone
- **Classification**: coastal, island, urbanization degree
- **Postal**: list of associated CAP codes

## Example output

```
> get_comune("Roma")

{
  "codice_istat": "058091",
  "nome": "Roma",
  "codice_catastale": "H501",
  "popolazione": 2802399,
  "superficie_kmq": 1288.19,
  "altitudine": 20,
  "zona_altimetrica": "Pianura",
  "litoraneo": 1,
  "latitudine": 41.89332,
  "longitudine": 12.482932,
  "sigla_provincia": "RM",
  "provincia": "Roma",
  "regione": "Lazio",
  "cap": ["00118", "00119", "00120", ...]
}
```

## Cache

Data is saved locally on first use:

| OS | Path |
|----|------|
| macOS / Linux | `~/.cache/italy-opendata-mcp/italia.db` |
| Windows | `%LOCALAPPDATA%\italy-opendata-mcp\italia.db` |

To refresh data, use `refresh_dataset(force=True)`.

## Data coverage

| | Count |
|---|---|
| Regions | 20 |
| Provinces | 107 |
| Municipalities | 7,896 |
| With population | 7,896 |
| With coordinates | 7,889 |
| With geographic data | 7,519 |
| With postal codes | 7,887 |

## License

MIT

<!-- mcp-name: io.github.stucchi/italy-opendata -->
