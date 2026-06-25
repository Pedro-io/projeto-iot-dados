#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT_DIR"

echo "==> Parando stack: infra/docker/docker-compose.yml"
if [ -f .env ]; then
  ENV_OPT="--env-file .env"
else
  ENV_OPT=""
fi

read -r -p "Remover volumes também? (y/N) " RESP
if [[ "$RESP" =~ ^[yY](es)?$ ]]; then
  echo "Removendo containers e volumes..."
  docker compose $ENV_OPT -f infra/airflow/docker-compose.yaml down -v --remove-orphans
  docker compose $ENV_OPT -f infra/docker/docker-compose.yml down -v --remove-orphans
  docker compose -f infra/kafka/docker-compose.yml down -v --remove-orphans
else
  echo "Removendo apenas containers (sem volumes)..."
  docker compose $ENV_OPT -f infra/airflow/docker-compose.yaml down --remove-orphans
  docker compose $ENV_OPT -f infra/docker/docker-compose.yml down --remove-orphans
  docker compose -f infra/kafka/docker-compose.yml down --remove-orphans
fi

echo "==> Operação concluída."
