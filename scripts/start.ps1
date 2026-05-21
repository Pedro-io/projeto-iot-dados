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

# Verifica processos terraform em execução e se o state está bloqueado
$tfProcs = Get-Process -Name terraform -ErrorAction SilentlyContinue
if ($tfProcs) {
    Write-Host "Processo(s) Terraform encontrado(s):"
    $tfProcs | Format-Table Id, ProcessName -AutoSize
    $ans = Read-Host "Deseja finalizar esses processos? (s/N)"
    if ($ans -match '^[sS]') {
        $tfProcs | Stop-Process -Force
        Write-Host "Processos Terraform finalizados."
    } else {
        Write-Host "Operação cancelada pelo usuário."
        exit 1
    }
}

function Test-FileLocked {
    param([string]$Path)
    try {
        $stream = [System.IO.File]::Open($Path, [System.IO.FileMode]::Open, [System.IO.FileAccess]::ReadWrite, [System.IO.FileShare]::None)
        $stream.Close()
        return $false
    } catch {
        return $true
    }
}

$statePath = Join-Path $Root 'infra/terraform/terraform.tfstate'
if (Test-Path $statePath) {
    if (Test-FileLocked $statePath) {
        Write-Host "Atenção: terraform.tfstate parece estar bloqueado por outro processo."
        Write-Host "Considere checar handles com Resource Monitor ou reiniciar a máquina."
        $cont = Read-Host "Deseja continuar mesmo assim (pode falhar)? (s/N)"
        if ($cont -notmatch '^[sS]') { exit 1 }
    }
}

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
Write-Host "  Mongo Express -> http://localhost:8083"
Write-Host "  Adminer       -> http://localhost:8082"