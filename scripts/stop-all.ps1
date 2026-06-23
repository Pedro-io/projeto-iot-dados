$ErrorActionPreference = "Stop"

$Root = Split-Path -Parent $PSScriptRoot
Set-Location $Root

Write-Host "==> Parando stack: infra/docker/docker-compose.yml"
if (Test-Path ".env") {
    $envOpt = "--env-file .env"
} else {
    $envOpt = ""
}

$resp = Read-Host "Remover volumes também? (s/N)"
if ($resp -match '^[sS](im)?$') {
    Write-Host "Removendo containers e volumes..."
    docker compose $envOpt -f infra/airflow/docker-compose.yaml down -v --remove-orphans
    docker compose $envOpt -f infra/docker/docker-compose.yml down -v --remove-orphans
    docker compose down -v --remove-orphans
} else {
    Write-Host "Removendo apenas containers (sem volumes)..."
    docker compose $envOpt -f infra/airflow/docker-compose.yaml down --remove-orphans
    docker compose $envOpt -f infra/docker/docker-compose.yml down --remove-orphans
    docker compose down --remove-orphans
}

Write-Host "==> Operação concluída."
