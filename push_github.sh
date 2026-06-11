#!/usr/bin/env bash
set -euo pipefail

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$DIR"

REPO_NAME="${1:-difusao-distribuida-sd}"
GITHUB_USER="${2:-LeandroRetz}"

# GitHub CLI (baixa automaticamente se necessário)
GH_BIN="/tmp/gh_2.63.2_linux_amd64/bin/gh"
if [[ ! -x "$GH_BIN" ]]; then
  echo "Baixando GitHub CLI..."
  curl -sL https://github.com/cli/cli/releases/download/v2.63.2/gh_2.63.2_linux_amd64.tar.gz -o /tmp/gh.tar.gz
  tar -xzf /tmp/gh.tar.gz -C /tmp
fi

if ! "$GH_BIN" auth status &>/dev/null; then
  echo "=============================================="
  echo " Autentique-se no GitHub (abre no navegador)"
  echo "=============================================="
  "$GH_BIN" auth login --hostname github.com --git-protocol https --web
fi

echo "Criando repositório $GITHUB_USER/$REPO_NAME e enviando código..."
"$GH_BIN" repo create "$REPO_NAME" \
  --public \
  --source=. \
  --remote=origin \
  --push \
  --description "Difusão Confiável e Atômica em Sistemas Distribuídos (Java/Sockets)"

echo ""
echo "Pronto! Repositório publicado em:"
"$GH_BIN" repo view --web 2>/dev/null || echo "https://github.com/$GITHUB_USER/$REPO_NAME"
