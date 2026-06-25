#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT_DIR"

if [ -f .env ]; then
  ENV_OPT="--env-file .env"
else
  ENV_OPT=""
fi

echo "==> Parando infraestrutura base..."
docker compose $ENV_OPT -f infra/docker/docker-compose.yml down --remove-orphans

echo "==> Infra base parada"
