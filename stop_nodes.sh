#!/usr/bin/env bash
set -euo pipefail

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$DIR/logs" 2>/dev/null || exit 0

for pidfile in nodo*.pid; do
  if [[ -f "$pidfile" ]]; then
    pid=$(cat "$pidfile")
    kill "$pid" 2>/dev/null || true
    rm -f "$pidfile"
    echo "Encerrado PID $pid ($pidfile)"
  fi
done
