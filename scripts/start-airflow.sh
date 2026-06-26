#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

echo "==> Verificando .env..."
if [ ! -f .env ]; then
  cp .env.template .env
  echo "    .env criado a partir do template. Edite as credenciais antes de continuar."
  exit 1
fi

echo "==> Inicializando banco de dados do Airflow..."
docker compose --env-file .env -f infra/airflow/docker-compose.yaml up airflow-init --wait

echo "==> Subindo Airflow (scheduler, worker, api-server)..."
docker compose --env-file .env -f infra/airflow/docker-compose.yaml up -d

echo "==> Airflow no ar"
docker compose --env-file .env -f infra/airflow/docker-compose.yaml ps
