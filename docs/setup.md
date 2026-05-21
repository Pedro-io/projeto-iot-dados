## Setup — Instalação e Execução

Guia completo para subir o ambiente do projeto do zero.

---

## Pré-requisitos

| Ferramenta | Versão mínima |
|---|---|
| Docker Desktop | 24.x |
| Python | 3.10+ |
| Poetry | 1.x |
| Git | 2.x |

> **Windows:** ao instalar o Python, marque obrigatoriamente **"Add Python to PATH"** antes de clicar em Install Now.

---

## 1. Clonar o repositório

```bash
git clone https://github.com/Pedro-io/projeto-iot-dados.git
cd projeto-iot-dados
```

---

## 2. Configurar variáveis de ambiente

Copie o template e preencha com suas credenciais:

```bash
cp .env.template .env
```

Edite `.env` com seus valores:

```bash
# MinIO
MINIO_ENDPOINT=http://localhost:9000
MINIO_ACCESS_KEY=minioadmin
MINIO_SECRET_KEY=minioadmin
DEFAULT_BUCKET=bronze

# Kafka
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
KAFKA_TOPIC=iot-sensors-raw
KAFKA_GROUP_ID=bronze-writer

# Consumer
DEFAULT_FLUSH_SIZE=100
DEFAULT_FLUSH_INTERVAL=30

# Schema Registry
DEFAULT_SCHEMA_REGISTRY_URL=http://localhost:8081
```

> **IMPORTANTE:** O arquivo `.env` está no `.gitignore`. Nunca comite credenciais no Git.

---

## 3. Configurar ambiente Python com Poetry

```bash
poetry config virtualenvs.in-project true
poetry install
```

**Alternativa sem Poetry:**

```bash
pip install kafka-python boto3 requests python-dotenv
```

---

## 4. Subir a infraestrutura

A infraestrutura é dividida em duas stacks independentes. Suba na ordem abaixo.

### 4a. Stack de storage (MongoDB, PostgreSQL, MinIO)

```bash
docker compose -f infra/docker/docker-compose.yml --env-file .env up -d
```

Detalhes: [infra/docker/README.md](../infra/docker/README.md)

### 4b. Provisionar buckets no MinIO via Terraform

```bash
cd infra/terraform
terraform init
terraform apply -auto-approve
cd ../..
```

Isso cria os buckets `bronze`, `silver` e `gold` no MinIO. Detalhes: [infra/terraform/README.md](../infra/terraform/README.md)

### 4c. Stack de streaming (Kafka + Schema Registry)

```bash
docker compose up -d
```

Verifique o status após 30 segundos:

```bash
docker compose ps
```

Saída esperada:

```
NAME                   STATUS
kafka                  Up (healthy)
schema-registry        Up (healthy)
schema-registry-init   Exited (0)
kafka-ui               Up
```

Detalhes: [docs/streaming.md](streaming.md)

---

## 5. Rodar o simulador de sensores

```bash
# Com Poetry
poetry run python src/ingestao/sensor_simulator.py --events-per-second 50 --anomaly-rate 0.1

# Sem Poetry
python src/ingestao/sensor_simulator.py --events-per-second 50 --anomaly-rate 0.1
```

---

## 6. Rodar o consumer Bronze (em outro terminal)

```bash
# Com Poetry
cd src && poetry run python -m streaming.consumer.bronze_consumer

# Sem Poetry
cd src && python -m streaming.consumer.bronze_consumer

# Modo dry-run (recomendado para testes — não grava no MinIO)
cd src && python -m streaming.consumer.bronze_consumer --dry-run
```

---

## 7. Verificar os dados

| Interface | URL | O que verificar |
|---|---|---|
| Kafka UI | http://localhost:8080 | Topics → iot-sensors-raw → Messages |
| Schema Registry | http://localhost:8081/subjects | Schema registrado |
| MinIO Console | http://localhost:9001 | Bucket `bronze` → pastas `factory_id=...` |
| Mongo Express | http://localhost:8083 | Coleção `equipments` (requer infra/docker stack) |

---

## 8. Rodar os testes

```bash
# Da raiz do projeto
PYTHONPATH=src python -m unittest discover -s tests -v

# Com Poetry
poetry run python -m unittest discover -s tests -v
```

---

## 9. Derrubar o ambiente

Execute na ordem abaixo para encerrar tudo de forma limpa.

### 9a. Parar os processos Python

Nos terminais onde o simulador e o consumer estão rodando, pressione `Ctrl+C`.

### 9b. Derrubar a stack de streaming (Kafka + Schema Registry)

Na raiz do projeto:

```bash
# Remove containers e volumes (dados do Kafka são descartados)
docker compose down -v

# Se quiser manter os dados do Kafka para a próxima sessão, omita o -v:
docker compose down
```

### 9c. Derrubar a stack de storage (MongoDB, PostgreSQL, MinIO)

```bash
# Remove containers e volumes (todos os dados são descartados)
docker compose -f infra/docker/docker-compose.yml --env-file .env down -v

# Se quiser manter os dados persistidos, omita o -v:
docker compose -f infra/docker/docker-compose.yml --env-file .env down
```

### 9d. (Opcional) Destruir recursos do Terraform

Só necessário se quiser remover os buckets criados no MinIO:

```bash
cd infra/terraform
terraform destroy -auto-approve
cd ../..
```

> **Atenção:** `terraform destroy` apaga os buckets `bronze`, `silver` e `gold` e todo o conteúdo deles no MinIO. Só execute se tiver certeza.

### Resumo rápido

| O que derrubar | Comando |
|---|---|
| Só streaming | `docker compose down -v` |
| Só storage | `docker compose -f infra/docker/docker-compose.yml --env-file .env down -v` |
| Tudo | Os dois comandos acima, nessa ordem |
| Buckets MinIO (Terraform) | `cd infra/terraform && terraform destroy -auto-approve` |
