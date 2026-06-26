# Runbook Operacional - Plataforma IoT Lakehouse

Guia de operação para subir, operar, monitorar e derrubar o ambiente do projeto.

---

## 1. Subir o Ambiente Completo

### Pré-requisitos

- Docker Desktop 24.x rodando
- Python 3.10+ com Poetry instalado
- Terraform instalado (para provisionamento de buckets)
- Arquivo `.env` configurado na raiz (copiar de `.env.template`)

### Sequência de inicialização

**Opção 1 - Script automatizado (recomendado):**

```bash
# Linux/Mac
./scripts/start.sh

# Windows (PowerShell)
.\scripts\start.ps1
```

**Opção 2 - Manual (passo a passo):**

```bash
# 1. Storage (MongoDB, PostgreSQL, MinIO, Spark, Grafana)
docker compose --env-file .env -f infra/docker/docker-compose.yml up -d

# 2. Buckets MinIO via Terraform
cd infra/terraform && terraform init && terraform apply -auto-approve && cd ../..

# 3. Kafka + Schema Registry
docker compose -f infra/kafka/docker-compose.yml up -d

# 4. Airflow
docker compose --env-file .env -f infra/airflow/docker-compose.yaml up -d
```

### Verificação pós-inicialização

| Verificação | Comando / URL |
|---|---|
| Storage saudável | `docker compose --env-file .env -f infra/docker/docker-compose.yml ps` |
| Kafka healthy | `docker compose -f infra/kafka/docker-compose.yml ps` |
| Airflow healthy | http://localhost:8080 (login: airflow/airflow) |
| MinIO acessível | http://localhost:9001 |
| Spark worker registrado | http://localhost:8085 (seção Workers → ALIVE) |
| Schema registrado | `curl http://localhost:8081/subjects` → `["iot-sensors-raw-value"]` |
| Grafana operacional | http://localhost:3001 (login: admin/admin) |
| Buckets existem | MinIO Console → buckets `bronze`, `silver`, `gold` |

---

## 2. Operar o Pipeline

### 2.1 Streaming (ingestão contínua)

```bash
# Terminal 1 - Simulador de sensores
poetry run python src/ingestao/sensor_simulator.py --events-per-second 50 --anomaly-rate 0.1

# Terminal 2 - Consumer Bronze
cd src && poetry run python -m streaming.consumer.bronze_consumer
```

O consumer grava arquivos NDJSON particionados no bucket `bronze` do MinIO.

### 2.2 Pipeline Batch (Airflow)

**Via interface web:**
1. Acesse http://localhost:8080
2. Ative a DAG `pipeline_iot` (toggle ON)
3. Clique em **Trigger DAG** (ícone de play)

**Via CLI:**
```bash
# Trigger para data de hoje
docker exec -it airflow-airflow-scheduler-1 airflow dags trigger pipeline_iot

# Trigger para data específica (reprocessamento)
docker exec -it airflow-airflow-scheduler-1 \
  airflow dags trigger pipeline_iot --logical-date 2026-06-20T00:00:00+00:00
```

O pipeline executa: Bronze (9 tasks paralelas) → Silver (10 tasks paralelas) → Gold (dimensões → fatos).

### 2.3 Modo Dry-Run (sem infraestrutura)

```bash
# Simulador - imprime eventos sem enviar ao Kafka
python src/ingestao/sensor_simulator.py --dry-run --events-per-second 5

# Consumer - consome e valida sem gravar no MinIO
cd src && python -m streaming.consumer.bronze_consumer --dry-run
```

---

## 3. Monitorar

### Logs centralizados (Grafana + Loki)

Acesse http://localhost:3001 → **Explore** → datasource **Loki**.

```logql
# Todos os logs dos containers
{job="docker"}

# Filtrar por nível de erro
{job="docker"} |= "ERROR"

# Logs do Kafka
{job="docker", container="/kafka"}

# Logs do consumer Bronze
{job="docker"} |= "bronze"
```

### Status das DAGs (Airflow)

| O que verificar | Onde |
|---|---|
| Status das tasks | Airflow UI → DAG → Grid View |
| Logs de cada task | Airflow UI → task → Log |
| Logs do Spark | `docker logs spark-master` ou http://localhost:8085 |

### Dados no MinIO

| Bucket | O que contém |
|---|---|
| `bronze` | Dados brutos: NDJSON (Kafka) + Parquet (PG/Mongo) |
| `silver` | Delta Tables limpas, tipadas e deduplicadas |
| `gold` | Star schema dimensional: dimensões + fatos |

Verifique via MinIO Console (http://localhost:9001) ou CLI:
```bash
docker exec minio mc ls --recursive local/bronze/ | head -20
docker exec minio mc ls --recursive local/silver/ | head -20
docker exec minio mc ls --recursive local/gold/ | head -20
```

---

## 4. Troubleshooting

### Kafka

| Problema | Solução |
|---|---|
| `kafka is unhealthy` | Aguarde 30s (KRaft formata storage na 1ª subida) |
| `NoBrokersAvailable` | Verifique se Kafka está `Up (healthy)` com `docker compose -f infra/kafka/docker-compose.yml ps` |
| Schema Registry `404` | Consumer usa validação local como fallback automático |

### Airflow

| Problema | Solução |
|---|---|
| `docker: command not found` | Verifique volume `/usr/bin/docker` no worker (Dockerfile customizado) |
| `Cannot connect to Docker daemon` | Verifique volume `/var/run/docker.sock` no worker |
| `spark-master is not running` | Suba a stack de storage: `docker compose --env-file .env -f infra/docker/docker-compose.yml up -d` |
| `No transformed data!` | Sem dados no Bronze para a `execution_date`. Verifique o bucket |

### Spark

| Problema | Solução |
|---|---|
| Worker não aparece na UI | Aguarde ~30s; verifique logs com `docker logs spark-worker` |
| `UnsupportedFileSystemException` | Use `s3a://` nos paths, não `s3://` |
| Job travado | Verifique `docker logs -f spark-master` |

### Geral

| Problema | Solução |
|---|---|
| Conflito de porta 8081 | Mongo Express e Schema Registry usam a mesma porta. Suba uma stack por vez |
| `.env` não encontrado | Copie `.env.template` para `.env` e preencha |
| Bucket não existe | Execute `cd infra/terraform && terraform apply -auto-approve` |

---

## 5. Derrubar o Ambiente

### Script automatizado
```bash
./scripts/stop-all.sh
```

### Manual (ordem recomendada)

```bash
# 1. Parar processos Python (Ctrl+C nos terminais do simulador/consumer)

# 2. Airflow
docker compose --env-file .env -f infra/airflow/docker-compose.yaml down -v

# 3. Kafka
docker compose -f infra/kafka/docker-compose.yml down -v

# 4. Storage (MongoDB, PostgreSQL, MinIO, Spark)
docker compose --env-file .env -f infra/docker/docker-compose.yml down -v

# 5. (Opcional) Destruir buckets Terraform
cd infra/terraform && terraform destroy -auto-approve
```

> **Atenção:** O flag `-v` apaga os volumes Docker (dados persistidos). Omita para manter dados entre sessões.

---

## 6. Reprocessamento

Para reprocessar uma data específica no pipeline:

1. Acesse Airflow UI → DAG `pipeline_iot`
2. Clique em **Trigger DAG w/ config**
3. Informe a `logical_date` desejada
4. O pipeline executará Bronze → Silver → Gold para aquela data

Os ETLs Silver usam `upsert` (Delta Merge), então reprocessar a mesma data atualiza registros existentes sem duplicar.

---

## 7. Variáveis de Ambiente

Todas definidas no arquivo `.env` na raiz do projeto:

| Variável | Usada por |
|---|---|
| `MINIO_ROOT_USER` / `MINIO_ROOT_PASSWORD` | MinIO, Spark, Terraform |
| `POSTGRES_USER` / `POSTGRES_PASSWORD` / `POSTGRES_DB` | PostgreSQL, Airflow (Bronze tasks) |
| `MONGO_USER` / `MONGO_PASS` | MongoDB, Airflow (Bronze tasks) |
| `KAFKA_BOOTSTRAP_SERVERS` | Simulador, Consumer |
| `KAFKA_TOPIC` | Simulador, Consumer |
| `KAFKA_GROUP_ID` | Consumer |
| `DEFAULT_SCHEMA_REGISTRY_URL` | Consumer |
| `DEFAULT_FLUSH_SIZE` / `DEFAULT_FLUSH_INTERVAL` | Consumer |
