# Projeto IoT Lakehouse — Plataforma de Integração de Dados

Plataforma completa de integração de dados para monitoramento industrial, desenvolvida como Projeto Integrador da disciplina **Integração de Dados II (2026/1)** — PUC Minas.

O sistema ingere eventos de sensores IoT em tempo real via Apache Kafka, valida os schemas formalmente via Confluent Schema Registry, persiste os dados brutos em um data lakehouse (MinIO) com arquitetura medallion (Bronze → Silver → Gold) e emite logs estruturados em JSON para facilitar observabilidade e auditoria.

---

## Visão Geral da Arquitetura

```
Sensores IoT (Simulador)
        │
        ▼
  Apache Kafka
  Tópico: iot-sensors-raw
        │
        ├─────────────────────────────┐
        ▼                             ▼
  Consumer Bronze             Schema Registry
  (Python)                    Valida schema do evento
        │
        ▼
  MinIO — Camada Bronze
  factory_id=.../measurement_type=.../dt=.../batch_xxx.ndjson
```

A arquitetura segue o padrão **Medallion**:

| Camada | Descrição | Status |
|---|---|---|
| Bronze | Dados brutos em JSON Lines, append-only, particionados por fábrica, tipo e data | ✅ E1 |
| Silver | Dados limpos, tipados e deduplicados via Spark/Flink | 🔄 E2 |
| Gold | Agregações horárias e diárias para consumo analítico | 🔄 E2/E3 |

---

## Stack Tecnológica

| Categoria | Tecnologia | Versão |
|---|---|---|
| Containerização | Docker + Docker Compose | 24.x / 2.x |
| Streaming | Apache Kafka | 7.5.3 (Confluent) |
| Coordenação | Apache Zookeeper | 7.5.3 (Confluent) |
| Validação de Schema | Confluent Schema Registry | 7.5.3 |
| Object Storage | MinIO | latest |
| Monitoramento | Kafka UI | 0.7.2 |
| Linguagem | Python | 3.10+ |
| Bibliotecas Python | kafka-python, boto3, requests | latest |

---

## Estrutura do Repositório

```
projeto-iot-dados/
├── docker-compose.yml              # Infraestrutura completa
├── README.md                       # Este arquivo
├── docs/
│   ├── modelo-dados.md             # Setup + ADRs de modelagem 
│   └── arquitetura.md              # Diagrama e decisões arquiteturais
└── src/
    ├── ingestao/
    │   └── sensor_simulator.py     # Produtor Kafka — simula sensores IoT
    └── streaming/
        └── bronze_consumer.py      # Consumer Kafka → MinIO Bronze
```

---

## Serviços e Portas

| Serviço | URL | Credenciais | Descrição |
|---|---|---|---|
| Kafka Broker | `localhost:9092` | — | Recebe eventos dos sensores |
| Schema Registry | `http://localhost:8081` | — | Valida e versiona schemas |
| Kafka UI | `http://localhost:8080` | — | Interface web para tópicos e schemas |
| MinIO S3 API | `http://localhost:9000` | minioadmin / minioadmin | API de armazenamento |
| MinIO Console | `http://localhost:9001` | minioadmin / minioadmin | Interface web do lakehouse |

---

## Pré-requisitos

| Ferramenta | Versão mínima | Link |
|---|---|---|
| Docker Desktop | 24.x | docker.com/products/docker-desktop |
| Python | 3.10+ | python.org/downloads |
| Git | 2.x | git-scm.com/downloads |

> **Windows:** ao instalar o Python, marque obrigatoriamente **"Add Python to PATH"** antes de clicar em Install Now. Se o comando `pip` não for reconhecido no terminal, use sempre `python -m pip` no lugar.

---

## Instalação e Execução

### 1. Clonar o repositório

```bash
git clone https://github.com/equipe/projeto-iot-dados.git
cd projeto-iot-dados
```

### 2. Instalar dependências Python

```bash
python -m pip install kafka-python boto3 requests
```

### 3. Subir a infraestrutura

Com o Docker Desktop aberto e em execução:

```bash
docker compose up -d
```

Verifique o status após 30 a 60 segundos:

```bash
docker compose ps
```

Saída esperada:

```
NAME                   STATUS
zookeeper              Up (healthy)
kafka                  Up (healthy)
schema-registry        Up (healthy)
schema-registry-init   Exited (0)    ← normal, encerra após registrar o schema
kafka-ui               Up
minio                  Up (healthy)
minio-init             Exited (0)    ← normal, encerra após criar os buckets
```

> **Windows/WSL2:** o Kafka pode demorar até 2 minutos para inicializar. Se aparecer `unhealthy`, aguarde e verifique novamente com `docker compose ps`. O `restart: unless-stopped` faz os serviços dependentes reconectarem automaticamente.

### 4. Verificar o Schema Registry

Acesse no browser:

```
http://localhost:8081/subjects
```

Resposta esperada: `["iot-sensors-raw-value"]` — confirma que o schema do tópico foi registrado automaticamente pelo `schema-registry-init`.

### 5. Rodar o simulador de sensores

```bash
cd src/ingestao
python sensor_simulator.py --events-per-second 50 --anomaly-rate 0.1
```

### 6. Rodar o consumer Bronze em outro terminal

```bash
cd src/streaming
python bronze_consumer.py --flush-size 50 --flush-interval 10
```

O consumer conecta automaticamente ao Schema Registry em `http://localhost:8081` e valida cada evento antes de gravar. Se o Schema Registry estiver offline, o consumer usa validação local como fallback sem interromper o pipeline.

### 7. Verificar os dados

| Interface | URL | O que verificar |
|---|---|---|
| Kafka UI | http://localhost:8080 | Topics → iot-sensors-raw → Messages |
| Schema Registry | http://localhost:8081/subjects | Schema registrado |
| MinIO Console | http://localhost:9001 | Bucket bronze → pastas factory_id=... |

---

## Logs Estruturados

O consumer emite todos os logs em formato **JSON Lines** — um objeto JSON por linha. Isso permite integração direta com Elasticsearch, Loki, CloudWatch e similares sem nenhum parsing adicional.

Exemplo de log de gravação bem-sucedida:

```json
{
  "timestamp": "2025-03-29T14:30:00.123Z",
  "level": "INFO",
  "logger": "bronze_consumer",
  "message": "Bronze gravado",
  "key": "factory_id=FAB-SP-01/measurement_type=temperature/dt=2025-03-29/batch_20250329_143000.ndjson",
  "events": 50,
  "bytes": 12480,
  "kb": 12.2
}
```

Exemplo de log de anomalia detectada:

```json
{
  "timestamp": "2025-03-29T14:30:05.456Z",
  "level": "WARNING",
  "logger": "bronze_consumer",
  "message": "Anomalia detectada",
  "sensor_id": "SENS-00042",
  "equipment_id": "EQ-0012",
  "factory_id": "FAB-SP-01",
  "measurement_type": "temperature",
  "value": 138.5,
  "quality": "bad"
}
```

Exemplo de estatísticas parciais (emitidas a cada flush):

```json
{
  "timestamp": "2025-03-29T14:30:30.789Z",
  "level": "INFO",
  "logger": "bronze_consumer",
  "message": "Estatísticas parciais",
  "received": 1500,
  "written": 1500,
  "anomalies": 152,
  "invalid": 0,
  "total_kb": 184.3,
  "elapsed_s": 30.1,
  "rate_per_s": 49.8
}
```

Todos os containers também têm rotação de logs configurada via driver `json-file` com limite de tamanho e número de arquivos, evitando crescimento ilimitado em disco.

---

## Particionamento no MinIO (Camada Bronze)

Os arquivos são gravados em formato **JSON Lines (.ndjson)** com particionamento Hive-style, compatível com leitura nativa pelo Apache Spark:

```
bronze/
  factory_id=FAB-SP-01/
    measurement_type=temperature/
      dt=2025-03-29/
        batch_20250329_143022.ndjson
    measurement_type=vibration/
      dt=2025-03-29/
        batch_20250329_143025.ndjson
  factory_id=FAB-MG-01/
    ...
  _dead_letter/
    dt=2025-03-29/
      invalid_20250329_143100.ndjson
```

O Spark pode ler toda a camada Bronze com filtros automáticos de partição:

```python
df = spark.read.json("s3a://bronze/")
df.filter("factory_id = 'FAB-SP-01' AND dt = '2025-03-29'")
```

---

## Parâmetros do Simulador

| Parâmetro | Padrão | Descrição |
|---|---|---|
| `--bootstrap-servers` | `localhost:9092` | Endereço do Kafka |
| `--topic` | `iot-sensors-raw` | Tópico de destino |
| `--equipments` | `50` | Número de equipamentos simulados |
| `--events-per-second` | `100` | Taxa de eventos por segundo |
| `--anomaly-rate` | `0.05` | Percentual de anomalias (0.0 a 1.0) |
| `--duration` | `0` | Duração em segundos (0 = infinito) |
| `--dry-run` | `false` | Imprime eventos sem enviar ao Kafka |

## Parâmetros do Consumer Bronze

| Parâmetro | Padrão | Descrição |
|---|---|---|
| `--bootstrap-servers` | `localhost:9092` | Endereço do Kafka |
| `--topic` | `iot-sensors-raw` | Tópico a consumir |
| `--group-id` | `bronze-writer` | Consumer group ID |
| `--minio-endpoint` | `http://localhost:9000` | Endpoint do MinIO |
| `--bucket` | `bronze` | Bucket de destino |
| `--flush-size` | `100` | Eventos por arquivo |
| `--flush-interval` | `30` | Segundos máximos entre gravações |
| `--schema-registry-url` | `http://localhost:8081` | URL do Schema Registry |
| `--dry-run` | `false` | Consome e valida sem gravar no MinIO |
| `--log-level` | `INFO` | Nível de log (DEBUG, INFO, WARNING, ERROR) |

---

## Decisões Técnicas Relevantes

**Schema Registry para validação formal:** o schema JSON do tópico `iot-sensors-raw` é registrado automaticamente na inicialização via `schema-registry-init`. O consumer consulta o Registry para validar campos obrigatórios e enums em tempo real. Em caso de falha do Registry, o consumer usa validação local como fallback — garantindo resiliência do pipeline.

**Commit manual de offset:** o consumer só confirma o offset após gravar com sucesso no MinIO. Se o processo cair no meio, as mensagens são reprocessadas — garantindo at-least-once delivery sem perda de dados.

**Chave de particionamento por `equipment_id`:** garante que eventos do mesmo equipamento sempre vão para a mesma partição Kafka, preservando a ordem cronológica necessária para análise de séries temporais no Silver.

**Dead-letter queue:** eventos com schema inválido são isolados em `bronze/_dead_letter/` com o motivo do erro registrado no campo `_dlq_reason`, facilitando auditoria e reprocessamento futuro sem travar o pipeline principal.

**Logs JSON estruturados:** todos os eventos de log incluem campos contextuais (`sensor_id`, `factory_id`, `events`, `bytes`, `rate_per_s`) além da mensagem, permitindo filtros e dashboards sem parsing de texto livre.

**Rotação de logs dos containers:** todos os serviços têm `logging.driver: json-file` com `max-size` e `max-file` configurados, evitando crescimento ilimitado dos logs em disco no ambiente local.

---

## Comandos Úteis

```bash
# Verificar status de todos os containers
docker compose ps

# Acompanhar logs do Kafka em tempo real
docker compose logs -f kafka

# Ver os últimos 10 logs do Schema Registry
docker compose logs schema-registry --tail=10

# Listar schemas registrados
curl http://localhost:8081/subjects

# Ver o schema completo do tópico
curl http://localhost:8081/subjects/iot-sensors-raw-value/versions/latest

# Listar arquivos gravados no Bronze
docker exec -it minio mc ls --recursive local/bronze

# Parar sem apagar dados
docker compose down

# Parar e apagar todos os volumes (reset completo)
docker compose down -v

# Testar simulador sem Kafka
python src/ingestao/sensor_simulator.py --dry-run --events-per-second 5

# Testar consumer sem MinIO
python src/streaming/bronze_consumer.py --dry-run --flush-size 10 --flush-interval 5
```

---

## Solução de Problemas

| Erro | Causa | Solução |
|---|---|---|
| `kafka is unhealthy` | Kafka ainda inicializando | Aguarde 2 minutos e rode `docker compose ps` |
| `NoBrokersAvailable` | Kafka não acessível | Verifique se o Docker está aberto e o Kafka está `Up` |
| `pip não reconhecido` | PATH do Python não configurado | Use `python -m pip install` no lugar de `pip install` |
| `docker não reconhecido` | Docker Desktop fechado | Abra o Docker Desktop e aguarde a baleia estabilizar |
| Schema Registry retorna `{}` | `schema-registry-init` ainda não rodou | Aguarde 30s e acesse `http://localhost:8081/subjects` novamente |
| Bucket bronze vazio | Consumer rodou com `--dry-run` | Rode sem a flag `--dry-run` |
| VS Code não reconhece `docker` ou `pip` | Ferramentas instaladas com VS Code aberto | Feche e reabra o VS Code após instalar |

---

## Integridade Acadêmica

Uso de IA generativa como ferramenta de apoio ao desenvolvimento. O entendimento do código é responsabilidade da equipe conforme política da disciplina.## Setup

Para executar este projeto, é necessário ter as seguintes ferramentas instaladas:

* Docker
* Python 3.14.3
* Poetry
* Terraform

Certifique-se de que todas estão corretamente configuradas antes de prosseguir.

---

## Estrutura de configuração

* O arquivo `.env` deve estar localizado na raiz do projeto
* O `docker-compose.yml` está em `infra/docker`
* Os scripts de infraestrutura (Terraform) estão em `infra/terraform`
* As dependências Python são gerenciadas pelo Poetry

Para mais detalhes:

* Infraestrutura Docker: `infra/docker/README.md`
* Infraestrutura com Terraform: `infra/terraform/README.md`

---

## Passos via Terminal

### 1. Configurar variáveis de ambiente

Copie o arquivo de exemplo:

```bash
cp .env.example .env
```

Edite o arquivo `.env` conforme necessário.

---

### 2. Inicializar a infraestrutura (Docker)

Certifique-se de que o Docker está em execução e execute:

```bash
docker-compose --env-file .env -f infra/docker/docker-compose.yml up -d
```

Para detalhes sobre os serviços (MongoDB, Postgres, MinIO, etc.), consulte:

```
infra/docker/README.md
```

---

### 3. Provisionar recursos com Terraform

Acesse a pasta de infraestrutura:

```bash
cd infra/terraform
```

Inicialize o Terraform:

```bash
terraform init
```

Visualize o plano das alterações que serão aplicadas:

```bash
terraform plan
```

Aplique a configuração:

```bash
terraform apply
```

Para mais detalhes sobre variáveis e recursos provisionados:

```
infra/terraform/README.md
```

---

### 4. Configurar ambiente Python

Configure o Poetry para criar o ambiente virtual dentro do projeto:

```bash
poetry config virtualenvs.in-project true
```

Instale as dependências:

```bash
poetry install
```

---

### 5. Executar Lint

Para verificar o código do projeto:

```bash
poetry run ruff check .
```

Para formatar o código:

```bash
poetry run ruff format .
```

---

### 6. Executar código Python

Para rodar qualquer script do projeto:

```bash
poetry run python src/main.py
```

---

## Observações

* O ambiente virtual `.venv` será criado automaticamente pelo Poetry
* O Docker utiliza o arquivo `.env` da raiz através da flag `--env-file`
* O Terraform é responsável por provisionar recursos no MinIO (como buckets)
* MongoDB e PostgreSQL são inicializados com dados básicos via scripts em `mongo-init` e `postgres-init`
* Não é necessário ativar manualmente o ambiente virtual ao utilizar `poetry run`
