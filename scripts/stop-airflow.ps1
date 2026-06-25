$ErrorActionPreference = "Stop"

$Root = Split-Path -Parent $PSScriptRoot
Set-Location $Root

if (Test-Path ".env") {
    $envOpt = "--env-file .env"
} else {
    $envOpt = ""
}

Write-Host "==> Parando Airflow..."
docker compose $envOpt -f infra/airflow/docker-compose.yaml down --remove-orphans

Write-Host "==> Airflow parado"
