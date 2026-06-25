$ErrorActionPreference = "Stop"

$Root = Split-Path -Parent $PSScriptRoot
Set-Location $Root

Write-Host "==> Subindo Kafka, Schema Registry e Kafka UI..."
docker compose -f infra/kafka/docker-compose.yml up -d

Write-Host "==> Kafka no ar"
docker compose -f infra/kafka/docker-compose.yml ps kafka schema-registry kafka-ui
