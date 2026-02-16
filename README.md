# italy-opendata-mcp

Server MCP che espone gli open data italiani (comuni, province, regioni, CAP, coordinate, dati geografici) attraverso tool semplici e developer-friendly.

## Caratteristiche

- **7 tool MCP** per navigare la gerarchia amministrativa italiana
- **Fonti ufficiali**: ISTAT e ANPR dove disponibili
- **Lazy download**: i dati vengono scaricati al primo utilizzo e salvati in cache locale (~1.8 MB SQLite)
- **Offline dopo il primo uso**: tutte le query sono locali
- **Nessun Docker**: installabile via `uvx` o `pip`, si avvia e termina con Claude

## Fonti dati

| Dato | Fonte | Tipo |
|------|-------|------|
| Comuni, province, regioni, codici ISTAT | [ISTAT Elenco Comuni](https://www.istat.it/classificazione/codici-dei-comuni-delle-province-e-delle-regioni/) | Ufficiale |
| Popolazione residente | [ANPR](https://github.com/italia/anpr-opendata) | Ufficiale (aggiornamento giornaliero) |
| Superficie, altitudine, zona altimetrica | [ISTAT Classificazioni](https://www.istat.it/classificazione/principali-statistiche-geografiche-sui-comuni/) | Ufficiale |
| CAP (codici postali) | [comuni-json](https://github.com/matteocontrini/comuni-json) | Community (nessuna fonte ufficiale disponibile) |
| Coordinate centroide | [opendatasicilia](https://github.com/opendatasicilia/comuni-italiani) | Community (nessuna fonte ufficiale disponibile) |

## Installazione

### Claude Desktop

Aggiungi al file di configurazione (`claude_desktop_config.json`):

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

### Da sorgente

```bash
git clone https://github.com/stucchi/italy-opendata-mcp.git
cd italy-opendata-mcp
uv venv && uv pip install -e .
```

## Tool disponibili

### Navigazione gerarchica

```
list_regioni()  →  list_province(regione="Lombardia")  →  list_comuni(provincia="MI")
```

| Tool | Parametri | Descrizione |
|------|-----------|-------------|
| `list_regioni` | — | Tutte le 20 regioni con numero comuni e popolazione |
| `list_province` | `regione?` | Province con filtro regione opzionale |
| `list_comuni` | `regione?`, `provincia?`, `limit?` | Comuni con filtri opzionali (default 400 risultati) |

### Ricerca

| Tool | Parametri | Descrizione |
|------|-----------|-------------|
| `get_comune` | `nome_o_codice` | Dettagli completi di un comune per nome o codice ISTAT |
| `get_by_cap` | `cap` | Trova i comuni associati a un codice postale |

### Gestione dati

| Tool | Parametri | Descrizione |
|------|-----------|-------------|
| `refresh_dataset` | `force?` | Ri-scarica i dati dalle fonti |
| `datasets_status` | — | Stato della cache locale |

## Campi disponibili per comune

Ogni comune include:

- **Anagrafica**: nome, codice ISTAT, codice catastale, sigla provincia, provincia, regione
- **Demografia**: popolazione (ANPR, aggiornamento giornaliero)
- **Geografia**: latitudine, longitudine, superficie km², altitudine (m), zona altimetrica
- **Classificazione**: litoraneo, isolano, grado di urbanizzazione
- **Postale**: lista CAP associati

## Esempio di output

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

I dati vengono salvati localmente al primo utilizzo:

| OS | Percorso |
|----|----------|
| macOS / Linux | `~/.cache/italy-opendata-mcp/italia.db` |
| Windows | `%LOCALAPPDATA%\italy-opendata-mcp\italia.db` |

Per aggiornare i dati, usa il tool `refresh_dataset(force=True)`.

## Copertura dati

| | Conteggio |
|---|---|
| Regioni | 20 |
| Province | 107 |
| Comuni | 7.896 |
| Con popolazione | 7.896 |
| Con coordinate | 7.889 |
| Con dati geografici | 7.519 |
| Con CAP | 7.887 |

## Licenza

MIT
