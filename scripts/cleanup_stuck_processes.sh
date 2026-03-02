#!/bin/bash
# Cleanup stuck DuckDB ingestion processes
# Run this manually if a process is stuck

set -e

SCRIPT_DIR="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)"
PROJECT_DIR="$(CDPATH= cd -- "$SCRIPT_DIR/.." && pwd)"
. "$PROJECT_DIR/scripts/lib/runtime_env.sh"
lh_load_runtime_env container

PID_FILE="${DB_PATH}.pid"

echo "🔍 Checking for stuck DuckDB processes..."
echo "=========================================="

# Check PID file
if [ -f "$PID_FILE" ]; then
    OLD_PID=$(cat "$PID_FILE")
    echo "📝 PID file found: $PID_FILE (PID: $OLD_PID)"

    # Check if process is running
    if ps -p "$OLD_PID" > /dev/null 2>&1; then
        echo "⚠️  Process $OLD_PID is RUNNING"

        # Show process info
        echo ""
        echo "Process details:"
        ps -p "$OLD_PID" -o pid,ppid,cmd,etime,stat

        echo ""
        read -p "Kill this process? [y/N] " -n 1 -r
        echo

        if [[ $REPLY =~ ^[Yy]$ ]]; then
            echo "🔪 Killing process $OLD_PID..."
            kill -9 "$OLD_PID" 2>/dev/null || true
            sleep 2

            if ps -p "$OLD_PID" > /dev/null 2>&1; then
                echo "❌ Failed to kill process"
                exit 1
            else
                echo "✅ Process killed"
            fi
        else
            echo "❌ Aborted"
            exit 1
        fi
    else
        echo "✅ Process $OLD_PID is NOT running (stale PID file)"
    fi

    # Remove PID file
    echo "🗑️  Removing PID file..."
    rm -f "$PID_FILE"
    echo "✅ PID file removed"
else
    echo "✅ No PID file found"
fi

# Check for WAL file (Write-Ahead Log = active connection)
WAL_FILE="${DB_PATH}.wal"
if [ -f "$WAL_FILE" ]; then
    echo ""
    echo "⚠️  DuckDB WAL file exists: $WAL_FILE"
    echo "   This indicates an active connection or crash"

    # Check for other Python processes accessing the DB
    echo ""
    echo "Searching for Python processes with 'liquidations.duckdb'..."
    PIDS=$(pgrep -f "liquidations.duckdb" 2>/dev/null || true)

    if [ -n "$PIDS" ]; then
        echo "⚠️  Found processes:"
        ps -p "$PIDS" -o pid,ppid,cmd,etime,stat

        echo ""
        read -p "Kill these processes? [y/N] " -n 1 -r
        echo

        if [[ $REPLY =~ ^[Yy]$ ]]; then
            echo "🔪 Killing processes..."
            kill -9 $PIDS 2>/dev/null || true
            sleep 2
            echo "✅ Processes killed"
        fi
    else
        echo "✅ No Python processes found accessing the database"
    fi

    echo ""
    echo "⚠️  WAL file still exists. You may need to:"
    echo "   1. Ensure no processes are using the DB"
    echo "   2. Try connecting with DuckDB CLI: duckdb $DB_PATH"
    echo "   3. If corrupted, restore from backup"
else
    echo "✅ No WAL file found"
fi

echo ""
echo "=========================================="
echo "✅ Cleanup complete"
echo ""
echo "You can now run the ingestion script again:"
echo "  python3 ${PROJECT_DIR}/ingest_full_history_safe.py [args]"
