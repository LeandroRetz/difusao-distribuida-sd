#!/usr/bin/env bash
set -euo pipefail

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$DIR"

echo "Compilando..."
javac *.java

mkdir -p logs

echo "Iniciando 3 nos (logs em ./logs/)..."
java DistributedNode config.json nodo1 > logs/nodo1.log 2>&1 &
echo $! > logs/nodo1.pid
sleep 1
java DistributedNode config.json nodo2 > logs/nodo2.log 2>&1 &
echo $! > logs/nodo2.pid
sleep 1
java DistributedNode config.json nodo3 > logs/nodo3.log 2>&1 &
echo $! > logs/nodo3.pid

echo "Nos iniciados:"
echo "  nodo1 PID $(cat logs/nodo1.pid) -> logs/nodo1.log"
echo "  nodo2 PID $(cat logs/nodo2.pid) -> logs/nodo2.log"
echo "  nodo3 PID $(cat logs/nodo3.pid) -> logs/nodo3.log"
echo ""
echo "Para interagir, anexe em um terminal:"
echo "  tail -f logs/nodo1.log"
echo ""
echo "Envie comandos via stdin do processo (use terminais separados):"
echo "  java DistributedNode config.json nodo1"
echo ""
echo "Parar todos: ./stop_nodes.sh"
