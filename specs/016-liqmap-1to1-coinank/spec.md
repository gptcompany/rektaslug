# Spec 016: Liq-Map 1:1 Coinank Visual Match

**Scope**: `frontend/liq_map_1w.html` - BTC/USDT 1W (poi ETH/USDT stessa pagina, stesso codice)
**Target**: Score >= 95% su `/validate-liqmap`
**Reference**: `.claude/commands/validate-liqmap.md` (checklist 32 elementi)

---

## Stato Attuale (baseline)

Il frontend attuale (`frontend/liq_map_1w.html`) ha questi gap rispetto a Coinank:

| Gap | Attuale | Target Coinank | Checklist ID |
|-----|---------|----------------|--------------|
| Chart type | Area chart (default era area, ora bar) | Stacked bars | T2-01 |
| Leverage groups | 5 individuali (5x,10x,25x,50x,100x) | 3 gruppi (Low/Medium/High) | T2-02 |
| Leverage colors | Cyan/Blue/DarkBlue/Orange/Pink | Blue/Purple/Orange | T2-03 |
| Cumulative fill | Solo linee, NO fill area | Filled area sotto entrambe le curve | T2-05, T2-06 |
| Current price | Solo dashed line + entry in legend | Label sopra + freccia UP + dot in basso | T2-09..11 |
| Background | Scuro (#0d1117) | Bianco (#ffffff) | T2-19 |
| Axis titles | "Liquidation Volume", "Price Level (USD)" | Nessun titolo, solo numeri con M/B | T2-12..16 |
| Chart title | "Binance BTC/USDT Perpetual..." visibile | Nessun titolo | T2-16 |
| Legend | 8 items (5 tier + 2 cumulative + price) | 3 items (Low/Medium/High), centered | T2-17..18 |
| Range slider | Assente | Presente sotto X-axis | T2-20 |
| X-axis format | Comma separator ($60,000) | Plain numbers (60160) | T2-14 |
| Grid | Scuro su scuro | Grigio chiaro su bianco | T2-21 |

## File da Modificare

**UN solo file**: `frontend/liq_map_1w.html`

Non servono modifiche a:
- Backend API (gia restituisce i dati corretti)
- Script di validazione (gia aggiornati con checklist e threshold 95%)
- Script Coinank screenshot (gia con capture_info)

## Implementazione Step-by-Step

### Step 1: Background bianco + font/grid (T2-19, T2-21)

Modificare il `layout` in `renderLiquidationMap()`:
```javascript
// DA:
paper_bgcolor: '#0d1117',
plot_bgcolor: '#0d1117',
font: { color: '#c9d1d9' },
gridcolor: '#30363d',

// A:
paper_bgcolor: '#ffffff',
plot_bgcolor: '#ffffff',
font: { color: '#333333', family: 'Arial, sans-serif' },
gridcolor: '#f0f0f0',
```

### Step 2: Rimuovere titoli assi e chart (T2-12..16)

```javascript
// Rimuovere da xaxis:
title: 'Price Level (USD)',   // ELIMINARE
tickformat: ',.0f'            // CAMBIARE in '.0f' (no comma)

// Rimuovere da yaxis:
title: 'Liquidation Volume',  // ELIMINARE

// Rimuovere da yaxis2:
title: 'Cumulative Liquidation Leverage',  // ELIMINARE

// Nascondere H1 e metadata:
document.getElementById('pageTitle').style.display = 'none';  // o display:none in HTML
document.getElementById('currentPrice').style.display = 'none';  // evita label duplicata fuori chart
```

### Step 3: 3 Leverage Groups (T2-02..04)

Sostituire `LEVERAGE_COLORS` con:
```javascript
const LEVERAGE_GROUPS = {
    'Low leverage':    { tiers: ['5x', '10x'],   color: '#5B8FF9' },  // Blue
    'Medium leverage': { tiers: ['25x', '50x'],  color: '#B37FEB' },  // Purple
    'High leverage':   { tiers: ['100x'],         color: '#FF9C6E' },  // Orange
};
```

Sostituire `groupByLeverage()` con funzione che aggrega per gruppo.
Aggiornare `buildBarTraces()` per usare i 3 gruppi.

### Step 4: Cumulative fill areas (T2-05..08)

Aggiungere `fill` e `fillcolor` alle trace cumulative:
```javascript
// Cumulative Long:
fill: 'tozeroy',
fillcolor: 'rgba(232, 104, 74, 0.12)',
line: { color: '#E8684A', width: 2 },
showlegend: false,

// Cumulative Short:
fill: 'tozeroy',
fillcolor: 'rgba(90, 216, 166, 0.12)',
line: { color: '#5AD8A6', width: 2 },
showlegend: false,
```

### Step 5: Current price annotation (T2-09..11)

Sostituire la trace scatter del current price con:
```javascript
// 1. Shape per la linea dashed (full height)
shapes: [{
    type: 'line',
    x0: currentPrice, x1: currentPrice,
    y0: 0, y1: 1, yref: 'paper',
    line: { color: '#ff0000', width: 1.5, dash: 'dash' }
}]

// 2. Annotation per label + freccia
annotations: [{
    x: currentPrice, y: 1.06,
    xref: 'x', yref: 'paper',
    text: `Current Price：${Math.round(currentPrice)}`,
    showarrow: true, arrowhead: 2,
    arrowcolor: '#ff0000', arrowwidth: 2,
    ax: 0, ay: 25,
    font: { color: '#333', size: 13 }
}]

// 3. Scatter marker per dot rosso in basso
{ x: [currentPrice], y: [0], mode: 'markers',
  marker: { color: '#ff0000', size: 8 },
  showlegend: false, hoverinfo: 'skip' }
```

Rimuovere il current price dalla trace scatter esistente e dalla legend.
Importante: il prezzo corrente NON deve rimanere anche nel DOM fuori dal grafico
(`id="currentPrice"`), altrimenti resta un doppione non presente in Coinank.

### Step 6: Legend 3 items centered (T2-17..18)

```javascript
legend: {
    orientation: 'h',
    yanchor: 'bottom', y: 1.02,
    xanchor: 'center', x: 0.5,
    font: { size: 13 },
    bgcolor: 'rgba(255,255,255,0)',
}
```

Le cumulative lines hanno `showlegend: false`.
Il current price non e piu una trace (e un'annotation).

### Step 7: Range slider (T2-20)

```javascript
xaxis: {
    rangeslider: { visible: true, thickness: 0.05 },
}
```

### Step 8: Body style bianco

```html
<body style="background: #fff; margin: 0; padding: 0;">
```

## Validazione

### Pre-requisito: Freshness dati (< 5 minuti, gate operativo)

Prima di OGNI validazione, i dati DuckDB devono essere aggiornati agli ultimi 5 minuti.
Senza questo, le discrepanze di volume/price range rispetto a Coinank (che usa dati live)
sono inevitabili e falsano il TIER 3.

Nota importante: questo vincolo e oggi un requisito di processo, NON un blocco automatico
di `scripts/validate_liqmap_visual.py`. Il validator attuale segnala solo staleness molto piu
ampia (>24h), quindi la verifica "< 5 min" va confermata manualmente prima del confronto.

Nota architetturale: `ccxt-data-pipeline` e una sorgente upstream che mantiene aggiornato il
catalogo Parquet. Non scrive direttamente in DuckDB. Il passaggio Parquet -> DuckDB avviene
tramite `scripts/fill_gap_from_ccxt.py` (o fase 6 di `scripts/run-ingestion.sh`), quindi la
freshness in DuckDB dipende anche da quel bridge, non solo dal daemon upstream.

```bash
# 1. Eseguire ingestion aggiornata
dotenvx run -f /media/sam/1TB/.env -- \
  uv run python scripts/daily_ingestion.py --symbol BTCUSDT

# 2. Verificare freshness
curl -s http://localhost:8002/data/date-range?symbol=BTCUSDT | python -m json.tool
# end_date deve essere < 5 min fa

# 3. Rigenerare cache heatmap/levels se necessario
# /refresh-heatmap --symbol BTCUSDT --validate
```

Se serve un refresh ravvicinato prima della validazione visuale, eseguire anche il bridge
dal catalogo ccxt verso DuckDB:

```bash
dotenvx run -f /media/sam/1TB/.env -- \
  uv run python scripts/fill_gap_from_ccxt.py --symbols BTCUSDT ETHUSDT
```

### Dopo ogni step, verificare visivamente:

I link locali seguono lo stesso schema URL di Coinank per confronto diretto:
```bash
# 1. Avviare il server (se non gia attivo)
uv run uvicorn src.liquidationheatmap.api.main:app --host 0.0.0.0 --port 8002

# 2. Link locali (stile Coinank):
# BTC 1W: http://localhost:8002/chart/derivatives/liq-map/binance/btcusdt/1w
# ETH 1W: http://localhost:8002/chart/derivatives/liq-map/binance/ethusdt/1w

# 3. Link Coinank corrispondenti (per confronto manuale):
# BTC 1W: https://coinank.com/chart/derivatives/liq-map/binance/btcusdt/1w
# ETH 1W: https://coinank.com/chart/derivatives/liq-map/binance/ethusdt/1w
```

### Dopo tutti gli step, validazione formale:

```bash
# BTC 1W - validazione completa con screenshot Coinank
dotenvx run -f /media/sam/1TB/.env -- \
  uv run python scripts/validate_liqmap_visual.py \
  --exchange binance --coin BTC --coinank-timeframe 1w

# Output: screenshot + manifest in data/validation/liqmap/ e manifests/
# Poi lanciare: /validate-liqmap
# che passa gli screenshot ad alpha-visual per scoring element-by-element

# ETH 1W - stessa pagina, diverso parametro
dotenvx run -f /media/sam/1TB/.env -- \
  uv run python scripts/validate_liqmap_visual.py \
  --exchange binance --coin ETH --coinank-timeframe 1w \
  --symbol ETHUSDT
```

### Comandi di validazione disponibili:

| Comando | Scope | Descrizione |
|---------|-------|-------------|
| `/validate-liqmap` | Liq-map BTC | Screenshot + alpha-visual comparison, threshold >= 95% |
| `/validate-liqmap --coin ETH --symbol ETHUSDT` | Liq-map ETH | Stessa pipeline, diverso symbol |
| `/validate-heatmap` | Heatmap 30d | Separato, NON in scope per questa spec |
| `/dashboard-check` | Sistema | Health check API + DB + render |
| `/validate` | Globale | 14-dimension ValidationOrchestrator |

## Comandi Nuova Sessione

```bash
# Opzione A: SpecKit pipeline (raccomandato)
/pipeline:speckit 016

# Opzione B: Implementazione diretta step-by-step
# Leggere questo file, implementare step 1-8, validare con /validate-liqmap
```

## File di Riferimento

| File | Ruolo |
|------|-------|
| `frontend/liq_map_1w.html` | **UNICO file da modificare** |
| `.claude/commands/validate-liqmap.md` | Checklist 32 elementi + prompt alpha-visual |
| `scripts/validate_liqmap_visual.py` | Pipeline screenshot + manifest (default: bar) |
| `scripts/coinank_screenshot.py` | Download nativo Coinank con capture_info |
| `data/validation/liqmap/coinank_binance_btcusdt_1w_*.png` | Screenshot riferimento Coinank |
| `data/validation/liqmap/ours_binance_btcusdt_1w_*.png` | Screenshot nostro (pre-fix) |
| `data/validation/manifests/liqmap_binance_btcusdt_1w_*.json` | Manifest con metriche numeriche |
| `elementi.png` | Coinank con tooltip (mostra struttura elementi interni) |

## Note

- ETH NON richiede spec separata: `liq_map_1w.html` e parametrico via `?symbol=ETHUSDT`
- Il backend API (`/liquidations/levels`) supporta gia entrambi i symbol
- La checklist nel manifesto JSON e la stessa per BTC e ETH
- Le modifiche a `validate_liqmap_visual.py` (default bar, capture_info, checklist) sono gia committate in `5aacb04`
