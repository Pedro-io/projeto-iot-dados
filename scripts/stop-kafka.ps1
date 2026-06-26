$ErrorActionPreference = "Stop"

$Root = Split-Path -Parent $PSScriptRoot
Set-Location $Root

Write-Host "==> Parando Kafka, Schema Registry e Kafka UI..."
docker compose -f infra/kafka/docker-compose.yml down --remove-orphans

Write-Host "==> Kafka parado"
