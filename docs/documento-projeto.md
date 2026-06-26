# Plataforma de Integração de Dados IoT
## Projeto Integrador - Integração de Dados II (2026/1)

**Instituição:** PUC Minas - Campus Lourdes  
**Curso:** Ciência de Dados e Inteligência Artificial  
**Disciplina:** Integração de Dados II  
**Repositório:** [github.com/Pedro-io/projeto-iot-dados](https://github.com/Pedro-io/projeto-iot-dados)

---

## Sumário

1. [Visão Geral](#1-visão-geral)
2. [Contexto de Negócio](#2-contexto-de-negócio)
3. [Arquitetura da Solução](#3-arquitetura-da-solução)
4. [Stack Tecnológica](#4-stack-tecnológica)
5. [Modelagem de Dados](#5-modelagem-de-dados)
6. [Implementação](#6-implementação)
7. [Orquestração com Airflow](#7-orquestração-com-airflow)
8. [Infraestrutura e DevOps](#8-infraestrutura-e-devops)
9. [Qualidade de Dados](#9-qualidade-de-dados)
10. [Observabilidade](#10-observabilidade)
11. [Como Executar](#11-como-executar)
12. [Decisões Arquiteturais (ADRs)](#12-decisões-arquiteturais-adrs)
13. [Competências Desenvolvidas](#13-competências-desenvolvidas)
14. [Referências](#14-referências)

---

## 1. Visão Geral

### 1.1 Objetivo

Desenvolver uma plataforma completa de integração de dados para um cenário de IoT (Internet of Things) industrial, aplicando conceitos de ingestão de dados de múltiplas fontes, armazenamento em data lakehouse, processamento batch e streaming, orquestração de pipelines e consumo analítico.

### 1.2 O que foi construído

Uma plataforma que:

- **Ingere** dados de sensores IoT em tempo real via Apache Kafka e dados de sistemas legados via batch
- **Armazena** dados brutos e processados em um data lakehouse com arquitetura Medallion (Bronze → Silver → Gold)
- **Processa** dados em lote com Apache Spark + Delta Lake, aplicando limpeza, deduplicação, tipagem e agregações
- **Orquestra** pipelines de forma automatizada e confiável com Apache Airflow
- **Disponibiliza** dados agregados em formato dimensional (star schema) para dashboards e análise

### 1.3 Números do Projeto

| Métrica | Valor |
|---|---|
| Containers Docker | 18 serviços |
| Tabelas Bronze | 10 (8 PostgreSQL + 1 MongoDB + 1 Kafka) |
| Delta Tables Silver | 10 tabelas com upsert e validação |
| Tabelas Gold | 7 (5 dimensões + 2 fatos) |
| Tasks na DAG Airflow | 26 tasks em 3 camadas |
| ADRs documentados | 9 decisões arquiteturais |
| Testes unitários | 35 testes |
| ETLs Spark | 26 jobs PySpark |

---

## 2. Contexto de Negócio

### 2.1 Cenário

Uma empresa de monitoramento industrial possui sensores distribuídos em fábricas que coletam dados de **temperatura, umidade, pressão, vibração e corrente**. Os sensores geram aproximadamente 1000 eventos por minuto, distribuídos entre 50 equipamentos em múltiplas fábricas.

### 2.2 Fontes de Dados

| Fonte | Tipo | Formato | Volume | Frequência |
|---|---|---|---|---|
| Sensores IoT (Simulador) | Streaming | JSON | ~1000 eventos/min | Tempo real |
| Sistema Legado (ERP) | Batch | PostgreSQL | ~100K registros | Diário |
| Cadastro de Equipamentos | Referência | MongoDB | ~50 documentos | Sob demanda |

### 2.3 Requisitos de Negócio

1. **Dashboard Operacional:** visualizar métricas em near real-time
2. **Alertas de Anomalia:** detectar leituras fora do range esperado
3. **Relatórios Diários:** agregações por fábrica, equipamento e tipo de sensor
4. **Histórico Completo:** manter dados brutos para auditoria e reprocessamento
5. **Análise Preditiva:** dados preparados em formato dimensional para modelos de ML

---

## 3. Arquitetura da Solução

### 3.1 Diagrama de Arquitetura

```
┌──────────────────────────────────────────────────────────────────────────┐
│                              FONTES DE DADOS                            │
├─────────────┬──────────────────┬────────────────┬────────────────────────┤
│  Sensores   │   PostgreSQL     │    MongoDB     │                        │
│  (Simulador)│   (ERP Legado)   │  (Equipamentos)│                        │
└──────┬──────┴──────┬───────────┴────────┬───────┴────────────────────────┘
       │             │                    │
       │ Streaming   │ Batch JDBC         │ Batch JDBC
       ▼             ▼                    ▼
┌──────────────────────────────────────────────────────────────────────────┐
│                         CAMADA DE INGESTÃO                               │
├──────────────────────┬───────────────────────────────────────────────────┤
│   Apache Kafka       │         Apache Spark (AbstractETL)                │
│   (KRaft mode)       │         Full load / Incremental                   │
│   Consumer → NDJSON  │         spark-submit → Parquet                    │
└──────────┬───────────┴──────────────────────┬────────────────────────────┘
           │                                   │
           ▼                                   ▼
┌──────────────────────────────────────────────────────────────────────────┐
│               BRONZE  -  MinIO bucket: bronze                            │
│                                                                          │
│  factory_id=*/measurement_type=*/dt=*/*.ndjson     (Kafka, append)       │
│  postgres/{8 tabelas}/                              (Parquet, append)     │
│  mongo/equipamentos/                                (Parquet, append)     │
│  _dead_letter/dt=*/                                 (eventos inválidos)   │
└──────────────────────────────┬───────────────────────────────────────────┘
                               │  Apache Spark + SilverETL
                               │  Limpeza, tipagem, deduplicação
                               ▼
┌──────────────────────────────────────────────────────────────────────────┐
│               SILVER  -  MinIO bucket: silver                            │
│               Formato: Delta Lake (transações ACID)                      │
│                                                                          │
│  postgres/{8 tabelas}/       (Delta, upsert)                             │
│  mongo/equipamentos/         (Delta, upsert)                             │
│  kafka/sensor_events/        (Delta, upsert, part. measurement_type)     │
└──────────────────────────────┬───────────────────────────────────────────┘
                               │  Apache Spark + GoldETL
                               │  Agregações dimensionais
                               ▼
┌──────────────────────────────────────────────────────────────────────────┐
│               GOLD  -  MinIO bucket: gold                                │
│               Formato: Delta Lake · Paradigma: Star Schema (Kimball)     │
│                                                                          │
│  Dimensões: dim_tempo, dim_fabrica, dim_equipamento,                     │
│             dim_sensor, dim_tipo_medicao                                  │
│  Fatos:     fct_leituras_hora (agregações horárias)                      │
│             fct_anomalias_dia (contagem diária de anomalias)             │
└──────────────────────────────┬───────────────────────────────────────────┘
                               │
                               ▼
┌──────────────────────────────────────────────────────────────────────────┐
│                       CAMADA DE ORQUESTRAÇÃO                             │
│                       Apache Airflow 3.2.2                               │
│              DAG: pipeline_iot - schedule @daily                          │
│       Bronze (9 tasks) → Silver (10 tasks) → Gold (7 tasks)             │
└──────────────────────────────────────────────────────────────────────────┘
                               │
                               ▼
┌──────────────────────────────────────────────────────────────────────────┐
│                       CAMADA DE CONSUMO                                   │
├──────────────────┬──────────────────┬────────────────────────────────────┤
│    Grafana       │     Trino        │        MinIO Console               │
│  (Logs/Alertas)  │   (Query Engine) │       (Exploração)                 │
└──────────────────┴──────────────────┴────────────────────────────────────┘
```

### 3.2 Arquitetura Medallion

A plataforma adota a **Arquitetura Medallion** com três camadas de qualidade crescente:

| Camada | Propósito | Formato | Modo de Escrita |
|---|---|---|---|
| **Bronze** | Dados brutos, imutáveis, rastreáveis | NDJSON (Kafka) / Parquet (PG/Mongo) | Append-only |
| **Silver** | Dados limpos, tipados, deduplicados | Delta Lake (Parquet + `_delta_log/`) | Upsert (Delta Merge) |
| **Gold** | Agregações analíticas prontas para consumo | Delta Lake | Upsert (Delta Merge) |

**Benefícios:**
- Rastreabilidade: dados brutos nunca são sobrescritos na Bronze
- Reprocessamento: qualquer camada pode ser reconstruída a partir da anterior
- Qualidade incremental: cada camada adiciona validações
- Transações ACID: Delta Lake garante integridade nas escritas Silver e Gold

---

## 4. Stack Tecnológica

### 4.1 Componentes Obrigatórios

| Categoria | Tecnologia | Versão | Justificativa |
|---|---|---|---|
| Containerização | Docker + Docker Compose | 24.x / 2.x | Ambiente reproduzível e isolado |
| Streaming | Apache Kafka (KRaft) | 7.5.3 | Alta throughput, ordenação por partição, replay |
| Orquestração | Apache Airflow | 3.2.2 | DAGs declarativas, scheduler, retry automático |
| Processamento | Apache Spark (Standalone) | 3.5.3 | PySpark, Delta Lake, processamento distribuído |
| Object Storage | MinIO | latest | Compatível S3A, paridade dev/produção |
| NoSQL | MongoDB | 8.2.6 | Schema flexível para cadastro de equipamentos |
| SQL | PostgreSQL | 16.13 | Schema 3FN para dados operacionais do ERP |

### 4.2 Componentes Adicionais

| Categoria | Tecnologia | Versão | Papel |
|---|---|---|---|
| Table Format | Delta Lake | 3.2.0 | Transações ACID sobre Spark |
| Qualidade de Dados | PyDeequ | 2.0.9 | Verificações de qualidade no pipeline |
| IaC | Terraform | latest | Provisionamento de buckets MinIO |
| Query Engine | Trino | 435 | Consultas SQL sobre dados no lakehouse |
| Observabilidade | Grafana + Loki + Promtail | 10.0 / 2.9 | Logs centralizados e dashboards |
| Schema Registry | Confluent Schema Registry | 7.5.3 | Validação e versionamento de schemas |
| Linguagem | Python | 3.11 (Spark) / 3.10+ (local) | Scripts ETL e consumer |

### 4.3 Mapa de Serviços e Portas

| Serviço | Porta | URL |
|---|---|---|
| Airflow Web UI | 8080 | http://localhost:8080 |
| Spark Master UI | 8085 | http://localhost:8085 |
| MinIO API (S3) | 9000 | http://localhost:9000 |
| MinIO Console | 9001 | http://localhost:9001 |
| Grafana | 3001 | http://localhost:3001 |
| Kafka UI | 8080 | http://localhost:8080 |
| Schema Registry | 8081 | http://localhost:8081 |
| Mongo Express | 8083 | http://localhost:8083 |
| Adminer (PostgreSQL) | 8082 | http://localhost:8082 |
| Trino | 8084 | http://localhost:8084 |
| Loki | 3100 | http://localhost:3100 |

---

## 5. Modelagem de Dados

### 5.1 Modelagem NoSQL - MongoDB

O cadastro de equipamentos utiliza MongoDB com **padrão de documento embutido** (Embedded Document / One-to-Few).

```json
{
  "_id": "EQ-1234",
  "name": "Compressor Industrial A1",
  "type": "compressor",
  "factory": {
    "id": "FAB-SP-01",
    "name": "Fábrica São Paulo",
    "location": {"lat": -23.5505, "lng": -46.6333}
  },
  "sensors": [
    {"id": "SENS-001-TEMP", "type": "temperature", "range": {"min": 0, "max": 150}},
    {"id": "SENS-002-VIBR", "type": "vibration", "range": {"min": 0, "max": 100}}
  ],
  "maintenance_schedule": "monthly",
  "installed_at": "2023-06-15T00:00:00Z",
  "status": "active"
}
```

**Justificativa:** Sensores são dependentes do equipamento (One-to-Few). Embutir a fábrica e sensores permite resolução atômica de contexto em uma única leitura, essencial para o enriquecimento no streaming.

**Índices:**
- `{ "factory.id": 1, "status": 1 }` — listagem por fábrica e status
- `{ "sensors.id": 1 }` — Multikey Index para resolução de telemetria recebida via Kafka

### 5.2 Modelagem Relacional - PostgreSQL (ERP Legado)

Schema normalizado em **3ª Forma Normal (3FN)** com 8 tabelas:

```
fabricas ──────────────┐
                       ├── equipamentos ──┐
tipos_equipamento ─────┘                  │
                                          ├── sensores ──┐
tipos_medicao ────────────────────────────┘              │
                                                         ├── leituras ──── metadados_leitura
status_qualidade ────────────────────────────────────────┘
```

- Triggers automáticos para `updated_at` em tabelas mutáveis
- Índices compostos em `(sensor_id, timestamp)` para consultas temporais
- `TIMESTAMPTZ` em todos os campos de data/hora

### 5.3 Modelagem Dimensional - Gold (Star Schema)

A camada Gold implementa um **Star Schema (Kimball)** com granularidade horária para fatos de leituras e diária para anomalias.

```
                         ┌─────────────────┐
                         │   dim_tempo      │
                         │ PK: date_key     │
                         │ data, hora, mês  │
                         └────────┬────────┘
                                  │
        ┌────────────┐            │            ┌──────────────────┐
        │ dim_fabrica│            │            │  dim_tipo_medicao│
        │ PK: fabrica_id          │            │ PK: tipo_medicao_id
        │ nome, lat, lng          │            │ nome, unidade     │
        └─────┬──────┘            │            │ faixa_normal      │
              │                   │            └────────┬─────────┘
              │      ┌────────────▼─────────────┐       │
              └──────│    fct_leituras_hora       │───────┘
                     │ avg, min, max, stddev      │
                     │ count_leituras             │
                     │ count_anomalias            │
                     │ pct_qualidade_boa          │
                     └───────────┬───────────────┘
                                 │
        ┌────────────────┐       │       ┌─────────────────┐
        │ dim_equipamento│       │       │   dim_sensor     │
        │ PK: equipment_id       │       │ PK: sensor_id    │
        │ nome, tipo, status     │       │ equipment_id     │
        │ periodicidade          │       │ tipo_medicao_id  │
        └────────────────┘       │       └─────────────────┘
                                 │
                     ┌───────────▼───────────────┐
                     │    fct_anomalias_dia       │
                     │ total_leituras             │
                     │ total_anomalias            │
                     │ taxa_anomalia              │
                     │ max/min_valor_dia          │
                     └───────────────────────────┘
```

**Tabelas Fato:**
- `fct_leituras_hora`: métricas horárias por sensor (avg, min, max, stddev, count)
- `fct_anomalias_dia`: contagem diária de anomalias e taxa por sensor

**Tabelas Dimensão:**
- `dim_tempo`: granularidade hora, gerada programaticamente
- `dim_fabrica`: fonte Silver/PostgreSQL (SCD Tipo 1)
- `dim_equipamento`: enriquecido com dados do MongoDB (periodicidade de manutenção)
- `dim_sensor`: fonte Silver/PostgreSQL (SCD Tipo 1)
- `dim_tipo_medicao`: fonte Silver/PostgreSQL com faixas operacionais

---

## 6. Implementação

### 6.1 Streaming - Consumer Bronze

O consumer Kafka segue princípios **SOLID** com separação clara de responsabilidades:

```
Kafka ──► BronzeConsumer
            ├── BaseConsumer (loop, commit, DLQ)
            ├── BronzeProcessor (enriquecimento)
            ├── SchemaValidator → SchemaRegistryClient (dual: remoto + local)
            ├── PartitionedBuffer (Hive-style)
            └── StorageClient → MinIO / DryRun
```

**Características:**
- Commit manual de offset (at-least-once delivery)
- Particionamento Hive-style: `factory_id/measurement_type/dt=YYYY-MM-DD/`
- Dead-letter queue para eventos inválidos
- Validação dual de schema (Registry remoto + fallback local)
- 35 testes unitários sem dependências externas

### 6.2 Processamento Batch - Apache Spark

Os ETLs seguem o padrão **Template Method** com herança:

```
AbstractETL (run, extract, transform, load, unit_tests)
  ├── Bronze ETLs (9 jobs)
  │     ├── PostgreSQL (8): fabricas, equipamentos, sensores, ...
  │     └── MongoDB (1): equipamentos
  ├── SilverETL (base)
  │     ├── PostgreSQL (8): limpeza, tipagem, upsert Delta
  │     ├── MongoDB (1): preservação de struct aninhado
  │     └── Kafka (1): achatamento de metadata, filtro por tipo
  └── GoldETL (base)
        ├── Dimensions (5): tempo, fabrica, equipamento, sensor, tipo_medicao
        └── Facts (2): fct_leituras_hora, fct_anomalias_dia
```

**Responsabilidades do `AbstractETL`:**
- Configuração automática de SparkSession com S3A
- Adição de colunas de metadados (`_execution_date`, `_processed_at`, `_data_source`)
- Validação de qualidade via PyDeequ antes de cada escrita
- Suporte a ambientes (`prd`, `hmg`) com resolução dinâmica de buckets
- Upsert Delta Lake com `whenMatchedUpdateAll` / `whenNotMatchedInsertAll`

### 6.3 Linhagem de Dados

```
PostgreSQL erp_legado
  └─ fabricas          → bronze/postgres/fabricas          → silver/postgres/fabricas          → dim_fabrica
  └─ equipamentos      → bronze/postgres/equipamentos      → silver/postgres/equipamentos      → dim_equipamento
  └─ tipos_equipamento → bronze/postgres/tipos_equipamento → silver/postgres/tipos_equipamento → dim_equipamento (join)
  └─ sensores          → bronze/postgres/sensores          → silver/postgres/sensores          → dim_sensor
  └─ tipos_medicao     → bronze/postgres/tipos_medicao     → silver/postgres/tipos_medicao     → dim_tipo_medicao
  └─ leituras          → bronze/postgres/leituras          → silver/postgres/leituras          ┐
  └─ metadados_leitura → bronze/postgres/metadados_leitura → silver/postgres/metadados_leitura ┘→ fct_leituras_hora
  └─ status_qualidade  → bronze/postgres/status_qualidade  → silver/postgres/status_qualidade    (lookup)

MongoDB equipments
  └─ coleção equipments → bronze/mongo/equipamentos → silver/mongo/equipamentos → dim_equipamento (enriquecimento)

Apache Kafka (sensor topic)
  └─ NDJSON Hive-partitioned → bronze/factory_id=*/measurement_type=*/dt=*/
      → silver/kafka/sensor_events → fct_leituras_hora
                                   → fct_anomalias_dia
```

---

## 7. Orquestração com Airflow

### 7.1 DAG: pipeline_iot

| Atributo | Valor |
|---|---|
| DAG ID | `pipeline_iot` |
| Schedule | `@daily` (meia-noite UTC) |
| Max active runs | 1 |
| Max active tasks | 1 |
| Catchup | Desativado |
| Tags | `iot`, `bronze`, `silver`, `gold` |

### 7.2 Fluxo de Execução

```
BRONZE (9 tasks em paralelo)
  ├── postgres_fabricas          ─┐
  ├── postgres_equipamentos       │
  ├── postgres_sensores           │
  ├── postgres_tipos_equipamento  ├── Ingestão batch via spark-submit
  ├── postgres_tipos_medicao      │   (docker exec no container spark-master)
  ├── postgres_status_qualidade   │
  ├── postgres_leituras           │
  ├── postgres_metadados_leitura  │
  └── mongo_equipamentos        ─┘
          │
          ▼ (todas as tasks Bronze devem concluir)
SILVER (10 tasks em paralelo)
  ├── postgres_fabricas          ─┐
  ├── postgres_equipamentos       │
  ├── postgres_sensores           │
  ├── postgres_tipos_equipamento  ├── Limpeza, tipagem, deduplicação
  ├── postgres_tipos_medicao      │   Upsert Delta Lake
  ├── postgres_status_qualidade   │
  ├── postgres_leituras           │
  ├── postgres_metadados_leitura  │
  ├── mongo_equipamentos          │
  └── kafka_sensor_events       ─┘
          │
          ▼ (todas as tasks Silver devem concluir)
GOLD
  ├── dimensions (5 tasks em paralelo)
  │     ├── dim_fabrica
  │     ├── dim_equipamento
  │     ├── dim_sensor
  │     ├── dim_tipo_medicao
  │     └── dim_tempo
  │           │
  │           ▼ (dimensões concluem primeiro)
  └── facts (2 tasks em paralelo)
        ├── fct_leituras_hora
        └── fct_anomalias_dia
```

Cada task executa um `BashOperator` que chama `docker exec` no container `spark-master`, invocando `spark-submit` com a data lógica da execução (`{{ ds }}`).

---

## 8. Infraestrutura e DevOps

### 8.1 Stacks Docker

O ambiente é composto por 3 stacks Docker Compose independentes:

| Stack | Arquivo | Serviços |
|---|---|---|
| **Storage** | `infra/docker/docker-compose.yml` | MongoDB, PostgreSQL, MinIO, Spark (Master + Worker), Grafana, Loki, Promtail, Trino, Hive Metastore |
| **Streaming** | `infra/kafka/docker-compose.yml` | Kafka (KRaft), Schema Registry, Schema Registry Init, Kafka UI |
| **Orquestração** | `infra/airflow/docker-compose.yaml` | Airflow (API Server, Scheduler, DAG Processor, Worker, Triggerer), PostgreSQL (Airflow), Redis |

### 8.2 Imagem Spark Customizada

A imagem Spark (`infra/docker/spark/Dockerfile`) usa build multistage:

1. **Base:** `apache/spark:3.5.3` — Spark pré-instalado
2. **Final:** `python:3.11-slim-bookworm` — Python 3.11 com:
   - OpenJDK 17 JRE
   - delta-spark 3.2.0, pydeequ, loguru
   - JARs: hadoop-aws, aws-java-sdk, delta-spark, delta-storage, deequ, mongo-spark-connector, postgresql

### 8.3 Imagem Airflow Customizada

A imagem Airflow (`infra/airflow/Dockerfile`) estende `apache/airflow:3.2.2` adicionando o binário Docker CLI para executar `docker exec` nas tasks.

### 8.4 Terraform (IaC)

Os buckets do data lake são gerenciados via Terraform com provider AWS apontando para o MinIO local:

```hcl
# Buckets: bronze, silver, gold
resource "aws_s3_bucket" "bronze" { bucket = "bronze" }
resource "aws_s3_bucket" "silver" { bucket = "silver" }
resource "aws_s3_bucket" "gold"   { bucket = "gold"   }
```

Credenciais lidas do `.env`, sem segredos hardcoded.

---

## 9. Qualidade de Dados

### 9.1 PyDeequ

Verificações de qualidade são executadas automaticamente por `AbstractETL` antes de cada escrita nas camadas Silver e Gold. Exemplos:

| Tabela | Verificação |
|---|---|
| `silver/postgres/fabricas` | `id`, `nome`, `latitude`, `longitude` completos (Error) |
| `silver/postgres/equipamentos` | `status ∈ {ativo, inativo, manutencao}` (Error) |
| `silver/kafka/sensor_events` | `quality ∈ {good, warning, bad}`, `measurement_type ∈ {temperature, humidity, pressure, vibration, current}` (Error) |
| `silver/postgres/tipos_medicao` | `valor_minimo < valor_maximo`, `faixa_normal_min < faixa_normal_max` (Error) |

### 9.2 Validação de Schema (Streaming)

O consumer Bronze implementa validação dual:

1. **Schema Registry remoto:** schema JSON centralizado como contrato produtor/consumidor
2. **Schema local (fallback):** schema embutido em `local_schema.py` para resiliência
3. **Dead-letter queue:** eventos inválidos isolados em `bronze/_dead_letter/` com `_dlq_reason`

### 9.3 Deduplicação

A camada Silver aplica deduplicação em todas as tabelas via `SilverETL._deduplicate(keys)`, removendo registros duplicados antes do upsert Delta.

---

## 10. Observabilidade

### 10.1 Stack de Logs

```
Containers Docker → Promtail (coleta) → Loki (armazena) → Grafana (visualiza)
```

- **Promtail** coleta logs de todos os containers via `/var/lib/docker/containers`
- **Loki** indexa logs por labels (container, job)
- **Grafana** permite consultas LogQL e criação de dashboards

### 10.2 Logs Estruturados

O consumer Python emite logs em **JSON Lines**, compatíveis com Loki sem parsing:

```json
{
  "timestamp": "2025-03-29T14:30:00.123Z",
  "level": "INFO",
  "logger": "bronze_consumer",
  "message": "Bronze gravado",
  "key": "factory_id=FAB-SP-01/measurement_type=temperature/dt=2025-03-29/batch_xxx.ndjson",
  "events": 50,
  "bytes": 12480
}
```

---

## 11. Como Executar

### 11.1 Pré-requisitos

- Docker Desktop 24.x
- Python 3.10+ com Poetry
- Terraform
- Git

### 11.2 Setup Rápido

```bash
# 1. Clonar
git clone https://github.com/Pedro-io/projeto-iot-dados.git
cd projeto-iot-dados

# 2. Configurar
cp .env.template .env
# Editar .env com credenciais

# 3. Instalar dependências Python
poetry install

# 4. Subir tudo
./scripts/start.sh          # Linux/Mac
.\scripts\start.ps1         # Windows
```

### 11.3 Demonstração do Fluxo Completo

```bash
# 1. Iniciar simulador de sensores (gera eventos Kafka)
poetry run python src/ingestao/sensor_simulator.py --events-per-second 50

# 2. Iniciar consumer Bronze (grava NDJSON no MinIO)
cd src && poetry run python -m streaming.consumer.bronze_consumer

# 3. Verificar dados no MinIO Console (http://localhost:9001)
#    → bucket "bronze" → pastas factory_id=.../measurement_type=.../dt=...

# 4. Executar pipeline batch no Airflow (http://localhost:8080)
#    → Ativar DAG "pipeline_iot" → Trigger DAG

# 5. Acompanhar execução no Airflow UI (Grid View)

# 6. Verificar dados processados
#    → MinIO: buckets "silver" e "gold" com Delta Tables

# 7. Consultar logs centralizados no Grafana (http://localhost:3001)
```

### 11.4 Derrubar o Ambiente

```bash
./scripts/stop-all.sh
```

---

## 12. Decisões Arquiteturais (ADRs)

O projeto documenta 9 ADRs em [docs/arquitetura.md](arquitetura.md):

| ADR | Decisão | Motivação Principal |
|---|---|---|
| ADR-001 | MongoDB para Cadastro de Equipamentos | Schema flexível, leitura atômica One-to-Few |
| ADR-002 | Apache Kafka no Modo KRaft | Alta throughput, sem ZooKeeper, replay de eventos |
| ADR-003 | Arquitetura Medallion com MinIO | Rastreabilidade, compatibilidade S3A dev/prod |
| ADR-004 | Apache Spark com Delta Lake | Transações ACID, Template Method, PyDeequ |
| ADR-005 | Schema Registry com Validação Dual | Resiliência (fallback local), DLQ para inválidos |
| ADR-006 | Loki + Promtail + Grafana | Stack leve, logs JSON, unificada com Grafana |
| ADR-007 | PostgreSQL para ERP Legado | 3FN, integridade referencial, TIMESTAMPTZ |
| ADR-008 | Terraform para Infraestrutura | IaC declarativa, idempotente, portável MinIO→S3 |
| ADR-009 | Padrão Silver com Classe Base | Template Method, mudanças propagam para 10 jobs |

---

## 13. Competências Desenvolvidas

| Competência | Como foi aplicada |
|---|---|
| **Modelagem de dados** | Schema NoSQL (MongoDB embedded), relacional 3FN (PostgreSQL), dimensional star schema (Gold) |
| **Arquitetura de dados** | Arquitetura Medallion com 3 camadas sobre object storage (MinIO/S3A) |
| **Integração** | 3 fontes heterogêneas: streaming (Kafka), batch JDBC (PostgreSQL), NoSQL (MongoDB) |
| **Processamento** | 26 jobs PySpark com Delta Lake, Template Method, PyDeequ |
| **Orquestração** | Apache Airflow com DAG de 26 tasks, schedule diário, retry automático |
| **Governança** | 9 ADRs, catálogo de dados, linhagem documentada, validação de qualidade |
| **Containerização** | 18 serviços Docker, Dockerfile multistage, IaC com Terraform |
| **Observabilidade** | Logs estruturados JSON, Grafana + Loki + Promtail |

---

## 14. Referências

### Documentação Oficial
- [Apache Kafka Documentation](https://kafka.apache.org/documentation/)
- [Apache Airflow Documentation](https://airflow.apache.org/docs/)
- [Apache Spark Documentation](https://spark.apache.org/docs/latest/)
- [Delta Lake Documentation](https://docs.delta.io/latest/)
- [MinIO Documentation](https://min.io/docs/minio/linux/index.html)
- [MongoDB Documentation](https://www.mongodb.com/docs/)
- [Terraform AWS Provider](https://registry.terraform.io/providers/hashicorp/aws/latest/docs)

### Repositórios de Referência
- [Data Engineering Zoomcamp](https://github.com/DataTalksClub/data-engineering-zoomcamp)
- [Awesome Data Engineering](https://github.com/igorbarinov/awesome-data-engineering)

### Conceitos Aplicados
- Databricks Medallion Architecture
- Kimball Dimensional Modeling (Star Schema)
- Template Method Pattern (Gang of Four)
- SOLID Principles
- Dead Letter Queue Pattern
