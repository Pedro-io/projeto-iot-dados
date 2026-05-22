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

# Carregar variáveis do .env
$envContent = Get-Content ".env" -Raw
$envVars = @{}
foreach ($line in $envContent -split "`n") {
    $line = $line.Trim()
    if ($line -and -not $line.StartsWith('#')) {
        $parts = $line -split '=', 2
        if ($parts.Length -eq 2) {
            $key = $parts[0].Trim()
            $value = $parts[1].Trim()
            if ($key -and $value) {
                $envVars[$key] = $value
            }
        }
    }
}

# Verifica processos terraform em execução
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

Write-Host "==> Provisionando buckets no MinIO via Terraform..."
Set-Location "infra/terraform"
& terraform init -input=false -reconfigure
& terraform apply `
    -var="minio_endpoint=$($envVars['MINIO_ENDPOINT'])" `
    -var="minio_access_key=$($envVars['MINIO_ROOT_USER'])" `
    -var="minio_secret_key=$($envVars['MINIO_ROOT_PASSWORD'])" `
    -auto-approve -input=false
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