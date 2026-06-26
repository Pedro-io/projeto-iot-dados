#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT_DIR"

echo "==> Parando Kafka, Schema Registry e Kafka UI..."
docker compose -f infra/kafka/docker-compose.yml down --remove-orphans

echo "==> Kafka parado"
