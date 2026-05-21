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

echo "==> Subindo storage + observabilidade (MongoDB, MinIO, Postgres, Loki, Promtail, Grafana)..."
docker compose --env-file .env -f infra/docker/docker-compose.yml up -d

echo "==> Aguardando healthchecks..."
sleep 10
docker compose -f infra/docker/docker-compose.yml ps

echo "==> Provisionando buckets no MinIO via Terraform..."
cd infra/terraform
terraform init -input=false -reconfigure
terraform apply -auto-approve -input=false
cd "$ROOT"

echo "==> Subindo Kafka + Schema Registry + Kafka UI..."
docker compose up -d

echo ""
echo "Stack no ar:"
echo "  Grafana      -> http://localhost:3001  (admin/admin)"
echo "  MinIO        -> http://localhost:9001"
echo "  Kafka UI     -> http://localhost:8080"
echo "  Mongo Express-> http://localhost:8083"
echo "  Adminer      -> http://localhost:8082"