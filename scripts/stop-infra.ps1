$ErrorActionPreference = "Stop"

$Root = Split-Path -Parent $PSScriptRoot
Set-Location $Root

if (Test-Path ".env") {
    $envOpt = "--env-file .env"
} else {
    $envOpt = ""
}

Write-Host "==> Parando infraestrutura base..."
docker compose $envOpt -f infra/docker/docker-compose.yml down --remove-orphans

Write-Host "==> Infra base parada"
