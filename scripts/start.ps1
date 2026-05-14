$ErrorActionPreference = "Stop"

$Root = Split-Path -Parent $PSScriptRoot
Set-Location $Root

Write-Host "==> Verificando .env..."
if (-not (Test-Path ".env")) {
    Copy-Item ".env.template" ".env"
    Write-Host "    .env criado a partir do template. Edite as credenciais antes de continuar."
    exit 1
}

Write-Host "==> Subindo storage + observabilidade (MongoDB, MinIO, Postgres, Loki, Promtail, Grafana)..."
docker compose --env-file .env -f infra/docker/docker-compose.yml up -d

Write-Host "==> Aguardando healthchecks..."
Start-Sleep -Seconds 10
docker compose -f infra/docker/docker-compose.yml ps

Write-Host "==> Provisionando buckets no MinIO via Terraform..."
Set-Location "infra/terraform"
terraform init -input=false -reconfigure
terraform apply -auto-approve -input=false
Set-Location $Root

Write-Host "==> Subindo Kafka + Schema Registry + Kafka UI..."
docker compose up -d

Write-Host ""
Write-Host "Stack no ar:"
Write-Host "  Grafana       -> http://localhost:3001  (admin/admin)"
Write-Host "  MinIO         -> http://localhost:9001"
Write-Host "  Kafka UI      -> http://localhost:8080"
Write-Host "  Mongo Express -> http://localhost:8081"
Write-Host "  Adminer       -> http://localhost:8082"