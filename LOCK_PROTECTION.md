# DuckDB Lock Protection System

## 🛡️ Multi-Level Lock Protection

Sistema di protezione a 3 livelli per prevenire processi stuck e conflitti di lock sul database DuckDB.

---

## **Livello 1: Safe Wrapper Script** ⚡

### File: `ingest_full_history_safe.py`

**Funzionalità**:
- ✅ **Lock Detection**: Controlla PID file e WAL file prima di iniziare
- ✅ **Timeout Protection**: Killa automaticamente processi che superano il limite
- ✅ **PID File Management**: Traccia processi attivi
- ✅ **Automatic Cleanup**: Rimuove PID file on exit (signal handler)

**Utilizzo**:
```bash
python3 ingest_full_history_safe.py \
  --symbol BTCUSDT \
  --data-dir /path/to/data \
  --db /path/to/db.duckdb \
  --mode full \
  --start-date 2021-12-01 \
  --end-date 2024-01-03 \
  --timeout 120  # Default: 120 minuti
```

**Output**:
```
🔒 Lock Detection
✅ No locks detected

📝 PID File Creation
✅ Created PID file: /path/to/db.duckdb.pid (PID 12345)

🚀 Execute Ingestion
⏱️  Starting with 120min timeout
[... output streaming in tempo reale ...]

✅ Completed successfully in 45m 32s
🗑️  Removed PID file
```

**Blocco se Lock Esiste**:
```bash
❌ BLOCKED: Another process has a lock on the database
   Resolution:
   1. Check if process is legitimate: ps aux | grep liquidations
   2. Kill if stuck: kill -9 3871 && rm /path/to/db.duckdb.pid
   3. Re-run this script
```

---

## **Livello 2: N8N Workflow Timeout** 🕒

### Node: `Execute DuckDB Ingest`

**Configurazione**:
```json
{
  "command": "python3 ingest_full_history_safe.py --timeout 120 ...",
  "continueOnFail": true,
  "executeOnce": false
}
```

**Workflow Settings**:
```json
{
  "executionTimeout": 7200  // 2 ore (120 min script + buffer)
}
```

**Comportamento**:
- Script ha **120 minuti** per completare
- Se supera 120min → script killa subprocess e ritorna exit code `124`
- N8N prosegue comunque (continueOnFail: true)
- Discord notification inviata con errore timeout

---

## **Livello 3: Manual Cleanup Script** 🧹

### File: `scripts/cleanup_stuck_processes.sh`

**Utilizzo**:
```bash
cd /media/sam/1TB/rektaslug
./scripts/cleanup_stuck_processes.sh
```

**Funzionalità**:
- ✅ Controlla PID file e verifica se processo è attivo
- ✅ Mostra dettagli processo (PID, tempo esecuzione, comando)
- ✅ Prompt interattivo prima di killare
- ✅ Ricerca processi Python che accedono al database
- ✅ Segnala presenza di WAL file (indicatore di crash)

**Output Esempio**:
```
🔍 Checking for stuck DuckDB processes...
==========================================
📝 PID file found: /workspace/2TB-NVMe/liquidationheatmap_db/liquidations.duckdb.pid (PID: 3871)
⚠️  Process 3871 is RUNNING

Process details:
 PID  PPID CMD                          ELAPSED STAT
3871  3820 python3 ingest_full_history  04:52:13 R

Kill this process? [y/N] y
🔪 Killing process 3871...
✅ Process killed
🗑️  Removing PID file...
✅ PID file removed

✅ Cleanup complete
```

---

## 🔧 Meccanismi di Lock Detection

### 1. PID File (`liquidations.duckdb.pid`)
- **Creato**: All'avvio dello script
- **Contenuto**: PID del processo
- **Rimosso**: On exit (anche con SIGTERM/SIGINT)
- **Utilità**: Tracking processo attivo

### 2. WAL File (`liquidations.duckdb.wal`)
- **Creato da**: DuckDB durante operazioni di scrittura
- **Indica**: Connessione attiva o crash
- **Utilità**: Detecting lock anche se PID file manca

### 3. Process Check (`os.kill(pid, 0)`)
- **Funzione**: Verifica se processo esiste
- **Fallback**: Se PID file esiste ma processo no → rimuovi PID file stale

---

## 🚨 Scenari di Gestione Lock

### Scenario A: Processo Stuck (>2 ore)
```
✅ Safe wrapper timeout (120min) → kill subprocess
✅ Exit code 124 → N8N marca come failed
✅ Discord notification: "DuckDB Ingest Failed - TIMEOUT"
✅ PID file rimosso automaticamente
```

### Scenario B: Processo Killato Manualmente (SIGTERM)
```
✅ Signal handler → cleanup PID file
✅ Exit gracefully
```

### Scenario C: Processo Crash (SIGKILL, kernel OOM)
```
⚠️  PID file rimane
⚠️  WAL file rimane

Prossima esecuzione:
❌ Lock detection → "Another process has lock"
👉 Risoluzione manuale: cleanup_stuck_processes.sh
```

### Scenario D: Doppia Esecuzione (workflow + manuale)
```
✅ Lock detection blocca secondo processo
✅ Messaggio chiaro: "Database locked by PID 12345"
✅ Nessun conflitto/corruzione database
```

---

## 📊 Exit Codes

| Exit Code | Significato | Azione N8N |
|-----------|-------------|------------|
| 0 | Success | Discord success ✅ |
| 1 | Lock detected / Generic error | Discord failure ❌ |
| 124 | Timeout | Discord failure ⏱️ |
| Other | Script error | Discord failure ❌ |

---

## 🔍 Troubleshooting

### "Database locked by PID XXXX"
```bash
# 1. Verifica se processo è legittimo
docker exec n8n-n8n-1 ps aux | grep XXXX

# 2. Se stuck, usa cleanup script
cd /media/sam/1TB/rektaslug
./scripts/cleanup_stuck_processes.sh

# 3. Oppure manuale
docker exec n8n-n8n-1 kill -9 XXXX
rm /media/sam/1TB/rektaslug//media/sam/2TB-NVMe/liquidationheatmap_db/liquidations.duckdb.pid
```

### "DuckDB WAL file exists"
```bash
# Check processi Python
docker exec n8n-n8n-1 pgrep -f liquidations.duckdb

# Se nessuno, il file è stale (da crash)
# Prova connessione per forzare checkpoint
docker exec n8n-n8n-1 python3 -c "import duckdb; duckdb.connect('/workspace/2TB-NVMe/liquidationheatmap_db/liquidations.duckdb').close()"

# WAL file dovrebbe sparire dopo connessione
```

### "Timeout after 120 minutes"
```
Possibili cause:
1. Dataset troppo grande → aumenta timeout: --timeout 240
2. Disco lento → check I/O con iostat
3. Lock contention → verifica altri processi
4. Script infinito → bug nel codice Python
```

---

## ✅ Best Practices

1. **Usa SEMPRE safe wrapper** in produzione
   ```bash
   # ✅ CORRETTO
   python3 ingest_full_history_safe.py --timeout 120 ...

   # ❌ EVITA (nessuna protezione lock)
   python3 ingest_full_history_n8n.py ...
   ```

2. **Set timeout appropriato**
   - Dataset piccolo (1 giorno): `--timeout 10`
   - Dataset medio (1 mese): `--timeout 60`
   - Dataset grande (1 anno): `--timeout 240`

3. **Monitoraggio**
   - Check Discord notifications
   - Review N8N execution logs
   - Grep per "TIMEOUT" o "BLOCKED"

4. **Cleanup regolare**
   - Run cleanup script se workflow fallisce
   - Non lasciare PID file stale

---

## 🔄 Integrazione N8N

Il workflow **"Binance Historical Data Downloader"** usa automaticamente il safe wrapper:

**Node**: `Execute DuckDB Ingest`
```bash
cd /workspace/1TB/LiquidationHeatmap && \
python3 ingest_full_history_safe.py \
  --symbol BTCUSDT \
  --data-dir /workspace/3TB-WDC/binance-history-data-downloader/data \
  --db /workspace/2TB-NVMe/liquidationheatmap_db/liquidations.duckdb \
  --mode full \
  --start-date "{{ $json.start_date }}" \
  --end-date "{{ $json.end_date }}" \
  --throttle-ms 200 \
  --timeout 120 \
  2>&1
```

**Protezioni Attive**:
- ✅ Lock detection prima di iniziare
- ✅ Timeout automatico 120min
- ✅ PID file tracking
- ✅ Cleanup automatico on exit
- ✅ Discord notifications

---

## 📝 Note Tecniche

### Perché 3 Livelli?

1. **Livello 1 (Safe Wrapper)**: Previene lock **prima** che inizino
2. **Livello 2 (Timeout)**: Killa processi stuck **durante** esecuzione
3. **Livello 3 (Cleanup)**: Recovery **dopo** crash/kill

### DuckDB Lock Behavior

DuckDB usa:
- **WAL (Write-Ahead Log)**: Per transazioni in corso
- **File Lock**: Per prevenire accessi concorrenti
- **Checkpoint**: Per mergiare WAL in main DB

Lock rilasciato:
- ✅ On `conn.close()`
- ✅ On process exit (Python __del__)
- ❌ NOT on crash (kernel OOM, SIGKILL)

---

## 🚀 Test del Sistema

### Test Lock Detection
```bash
# Terminal 1: Start long-running process
docker exec n8n-n8n-1 python3 /workspace/1TB/LiquidationHeatmap/ingest_full_history_safe.py \
  --symbol BTCUSDT --data-dir /workspace/3TB-WDC/binance-history-data-downloader/data \
  --db /workspace/2TB-NVMe/liquidationheatmap_db/liquidations.duckdb \
  --mode full --start-date 2021-12-01 --end-date 2024-01-03 --timeout 240

# Terminal 2: Try starting second process (should be blocked)
docker exec n8n-n8n-1 python3 /workspace/1TB/LiquidationHeatmap/ingest_full_history_safe.py \
  --symbol BTCUSDT --data-dir /workspace/3TB-WDC/binance-history-data-downloader/data \
  --db /workspace/2TB-NVMe/liquidationheatmap_db/liquidations.duckdb \
  --mode auto --timeout 10

# Expected output:
# ❌ BLOCKED: Another process has a lock on the database
```

### Test Timeout
```bash
# Start with short timeout (should timeout)
docker exec n8n-n8n-1 python3 /workspace/1TB/LiquidationHeatmap/ingest_full_history_safe.py \
  --symbol BTCUSDT --data-dir /workspace/3TB-WDC/binance-history-data-downloader/data \
  --db /workspace/2TB-NVMe/liquidationheatmap_db/liquidations.duckdb \
  --mode full --start-date 2021-12-01 --end-date 2024-01-03 --timeout 1

# Expected output after 1 minute:
# ❌ TIMEOUT after 1 minutes!
# Exit code: 124
```

---

## 📚 References

- **DuckDB Concurrency**: https://duckdb.org/docs/stable/connect/concurrency
- **Python Signal Handling**: https://docs.python.org/3/library/signal.html
- **N8N Timeout Settings**: https://docs.n8n.io/workflows/settings/

---

**Last Updated**: 2025-11-03
**Author**: Claude Code (Anthropic)
**Version**: 1.0
