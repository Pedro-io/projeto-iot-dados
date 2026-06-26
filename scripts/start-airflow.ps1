$ErrorActionPreference = "Stop"

$Root = Split-Path -Parent $PSScriptRoot
Set-Location $Root

Write-Host "==> Verificando .env..."
if (-not (Test-Path ".env")) {
    Copy-Item ".env.template" ".env"
    Write-Host "    .env criado a partir do template. Edite as credenciais antes de continuar."
    exit 1
}

Write-Host "==> Inicializando banco de dados do Airflow..."
docker compose --env-file .env -f infra/airflow/docker-compose.yaml up airflow-init --wait

Write-Host "==> Subindo Airflow (scheduler, worker, api-server)..."
docker compose --env-file .env -f infra/airflow/docker-compose.yaml up -d

Write-Host "==> Airflow no ar"
docker compose --env-file .env -f infra/airflow/docker-compose.yaml ps
