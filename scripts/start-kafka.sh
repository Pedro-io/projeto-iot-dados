#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

echo "==> Subindo Kafka, Schema Registry e Kafka UI..."
docker compose -f infra/kafka/docker-compose.yml up -d

echo "==> Kafka no ar"
docker compose -f infra/kafka/docker-compose.yml ps kafka schema-registry kafka-ui
