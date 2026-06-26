# Projeto IoT Lakehouse - Plataforma de Integração de Dados

Plataforma de integração de dados para monitoramento industrial, desenvolvida como Projeto Integrador da disciplina **Integração de Dados II (2026/1)** - PUC Minas.

O sistema ingere eventos de sensores IoT em tempo real via Apache Kafka, processa dados em lote com Apache Spark + Delta Lake seguindo a **arquitetura Medallion** (Bronze → Silver → Gold), orquestra os pipelines com Apache Airflow e disponibiliza dados agregados para consumo analítico em dashboards.

---

## Arquitetura

```
┌──────────────────────────────────────────────────────────────┐
│                        FONTES DE DADOS                       │
│  Sensores IoT       PostgreSQL ERP      MongoDB              │
│  (Simulador)        (erp_legado)        (equipments)         │
└──────┬──────────────────┬───────────────────┬────────────────┘
       │  Streaming        │  Batch JDBC        │  Batch JDBC
       ▼                   ▼                   ▼
┌─────────────────────────────────────────────────────────────────────┐
│                     CAMADA DE INGESTÃO                              │
│  Apache Kafka (KRaft)           spark-submit (AbstractETL)          │
│  consumer → NDJSON              full load / incremental             │
└──────┬──────────────────────────────────┬───────────────────────────┘
       │                                  │
       ▼                                  ▼
┌─────────────────────────────────────────────────────────────────────┐
│              BRONZE  -  MinIO (S3A)                                  │
│  NDJSON Hive-partitioned (Kafka)  ·  Parquet (PostgreSQL + MongoDB)  │
└──────────────────────────────┬──────────────────────────────────────┘
                               │  spark-submit (SilverETL)
                               ▼
┌─────────────────────────────────────────────────────────────────────┐
│              SILVER  -  Delta Lake                                    │
│  10 tabelas: upsert, deduplicação, tipagem forte, validação PyDeequ  │
└──────────────────────────────┬──────────────────────────────────────┘
                               │  spark-submit (GoldETL)
                               ▼
┌─────────────────────────────────────────────────────────────────────┐
│              GOLD  -  Star Schema Dimensional (Delta Lake)           │
│  5 dimensões + 2 fatos: agregações horárias e diárias                │
└──────────────────────────────┬──────────────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────────────┐
│                   CAMADA DE ORQUESTRAÇÃO                             │
│  Apache Airflow - DAG: pipeline_iot (@daily)                         │
│  Bronze (9 tasks) → Silver (10 tasks) → Gold (7 tasks)               │
└─────────────────────────────────────────────────────────────────────┘
```

### Status de Implementação

| Camada | Descrição | Status |
|---|---|---|
| Bronze | 8 tabelas Parquet (PG + Mongo) + NDJSON Kafka particionado | ✅ Completo |
| Silver | 10 Delta Tables com upsert, deduplicação e validação PyDeequ | ✅ Completo |
| Gold | Star schema dimensional com 5 dimensões e 2 tabelas fato | ✅ Completo |
| Airflow | DAG `pipeline_iot` com schedule diário e 26 tasks | ✅ Completo |
| Streaming | Consumer Bronze com validação de schema e DLQ | ✅ Completo |
| Observabilidade | Grafana + Loki + Promtail para logs centralizados | ✅ Completo |

---

## Stack Tecnológica

| Categoria | Tecnologia | Versão |
|---|---|---|
| Containerização | Docker + Docker Compose | 24.x / 2.x |
| Streaming | Apache Kafka (KRaft) + Schema Registry | 7.5.3 |
| Orquestração | Apache Airflow (CeleryExecutor) | 3.2.2 |
| Processamento | Apache Spark (Standalone) + Delta Lake | 3.5.3 / 3.2.0 |
| Object Storage | MinIO (compatível S3) | latest |
| NoSQL | MongoDB | 8.2.6 |
| SQL | PostgreSQL | 16.13 |
| Qualidade de Dados | PyDeequ | 2.0.9 |
| IaC | Terraform | latest |
| Observabilidade | Grafana + Loki + Promtail | 10.0 / 2.9 |
| Query Engine | Trino | 435 |
| Linguagem | Python 3.11 (Spark) / 3.10+ (local) | |

---

## Estrutura do Repositório

```
projeto-iot-dados/
├── README.md
├── docs/
│   ├── documento-projeto.md        # Documentação completa e apresentável
│   ├── arquitetura.md              # ADRs e decisões arquiteturais
│   ├── catalogo-dados.md           # Catálogo: schemas, linhagem, owners
│   ├── modelo-dados.md             # Modelagem MongoDB (NoSQL)
│   ├── dag-pipeline-iot.md         # Documentação da DAG Airflow
│   ├── runbook.md                  # Guia de operação
│   ├── setup.md                    # Instalação e execução passo a passo
│   ├── streaming.md                # Infraestrutura Kafka
│   ├── consumer-bronze.md          # Arquitetura SOLID do consumer
│   ├── spark-jobs.md               # Como rodar jobs Spark
│   ├── spark-dockerfile.md         # Imagem Docker do Spark
│   ├── observability.md            # Stack de logs (Grafana + Loki)
│   └── troubleshooting.md          # Erros comuns e comandos úteis
├── infra/
│   ├── docker/                     # Stack storage: MongoDB, PostgreSQL, MinIO, Spark, Grafana
│   │   ├── docker-compose.yml
│   │   ├── spark/Dockerfile
│   │   ├── postgres-init/          # Schema e seed do PostgreSQL
│   │   └── mongo-init/             # Init script do MongoDB
│   ├── kafka/                      # Stack streaming: Kafka, Schema Registry, Kafka UI
│   │   └── docker-compose.yml
│   ├── airflow/                    # Stack orquestração: Airflow (CeleryExecutor)
│   │   ├── docker-compose.yaml
│   │   └── Dockerfile
│   └── terraform/                  # Provisionamento dos buckets S3 no MinIO
├── src/
│   ├── ingestao/
│   │   └── sensor_simulator.py     # Produtor Kafka - simula sensores IoT
│   ├── streaming/                  # Consumer Bronze (SOLID)
│   │   ├── consumer/               # BaseConsumer + BronzeConsumer
│   │   ├── buffer/                 # PartitionedBuffer Hive-style
│   │   ├── schema/                 # SchemaRegistry + fallback local
│   │   ├── validation/             # SchemaValidator
│   │   ├── storage/                # MinIO client + DryRun
│   │   ├── logging/                # JSON formatter
│   │   └── tests/                  # 35 testes unitários
│   ├── processamento/              # Jobs Spark (ETL)
│   │   ├── abstract_etl.py         # Template Method base
│   │   ├── bronze/                 # 9 ETLs: postgres (8) + mongo (1)
│   │   ├── silver/                 # 10 ETLs: postgres (8) + mongo (1) + kafka (1)
│   │   │   └── base_silver_etl.py  # Classe base Silver
│   │   └── gold/                   # 7 ETLs: dimensions (5) + facts (2)
│   │       └── base_gold_etl.py    # Classe base Gold
│   └── dags/
│       └── pipeline_iot.py         # DAG Airflow: Bronze → Silver → Gold
├── observability/                  # Configs Grafana, Loki, Promtail
└── scripts/                        # Scripts de automação (start/stop)
```

---

## Quick Start

```bash
# 1. Clonar e instalar dependências
git clone https://github.com/Pedro-io/projeto-iot-dados.git
cd projeto-iot-dados
poetry install
```

**Subir toda a infraestrutura:**

```bash
# Linux/Mac
./scripts/start.sh

# Windows (PowerShell)
.\scripts\start.ps1
```

O script automaticamente:
1. Sobe storage (MongoDB, PostgreSQL, MinIO, Spark)
2. Provisiona buckets no MinIO via Terraform (`bronze`, `silver`, `gold`)
3. Sobe Kafka + Schema Registry
4. Inicializa e sobe o Airflow

**Serviços disponíveis após a inicialização:**

| Serviço | URL | Credenciais |
|---|---|---|
| Airflow | http://localhost:8080 | airflow / airflow |
| Grafana | http://localhost:3001 | admin / admin |
| MinIO Console | http://localhost:9001 | (definido no .env) |
| Kafka UI | http://localhost:8080 | - |
| Spark Master UI | http://localhost:8085 | - |
| Mongo Express | http://localhost:8083 | (definido no .env) |
| Adminer (PostgreSQL) | http://localhost:8082 | (definido no .env) |
| Trino | http://localhost:8084 | - |

**Rodar o simulador + consumer:**

```bash
# Simulador de sensores IoT
poetry run python src/ingestao/sensor_simulator.py --events-per-second 50

# Consumer Bronze (em outro terminal)
cd src && poetry run python -m streaming.consumer.bronze_consumer
```

**Executar o pipeline batch via Airflow:**

Acesse http://localhost:8080, ative a DAG `pipeline_iot` e clique em Trigger.

Guia completo: [docs/setup.md](docs/setup.md)

---

## Documentação

| Documento | Conteúdo |
|---|---|
| [docs/documento-projeto.md](docs/documento-projeto.md) | Documentação completa e apresentável do projeto |
| [docs/arquitetura.md](docs/arquitetura.md) | 9 ADRs e decisões arquiteturais |
| [docs/catalogo-dados.md](docs/catalogo-dados.md) | Catálogo de dados: schemas, linhagem, owners |
| [docs/modelo-dados.md](docs/modelo-dados.md) | Modelagem MongoDB (NoSQL) |
| [docs/dag-pipeline-iot.md](docs/dag-pipeline-iot.md) | DAG Airflow: pipeline_iot |
| [docs/runbook.md](docs/runbook.md) | Guia de operação |
| [docs/setup.md](docs/setup.md) | Instalação e execução passo a passo |
| [docs/streaming.md](docs/streaming.md) | Stack Kafka - serviços, portas, comandos |
| [docs/consumer-bronze.md](docs/consumer-bronze.md) | Arquitetura SOLID do consumer Bronze |
| [docs/spark-jobs.md](docs/spark-jobs.md) | Como rodar e testar jobs Spark |
| [docs/observability.md](docs/observability.md) | Stack de logs - Grafana, Loki, Promtail |
| [docs/troubleshooting.md](docs/troubleshooting.md) | Erros comuns e comandos úteis |

---

*Projeto acadêmico - PUC Minas 2026/1 - Integração de Dados II*
