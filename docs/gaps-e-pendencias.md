# Status do Projeto IoT Lakehouse

> Documento atualizado em 2026-06-26.

---

## O que está implementado

### E1 - Fundação (Completo)

| Componente | Localização | Status |
|---|---|---|
| Docker Compose funcional (Kafka, MongoDB, PostgreSQL, MinIO) | `infra/docker/`, `infra/kafka/` | ✅ |
| Simulador de sensores IoT (~1000 eventos/min) | `src/ingestao/sensor_simulator.py` | ✅ |
| Modelagem NoSQL MongoDB com documento de justificativa | `docs/modelo-dados.md` | ✅ |
| Consumer Kafka gravando dados brutos no MinIO (Bronze) | `src/streaming/consumer/` | ✅ |
| Buffer particionado Hive-style | `src/streaming/buffer/` | ✅ |
| Validação de schema (Registry + fallback local) | `src/streaming/schema/`, `validation/` | ✅ |
| Dead-letter queue (DLQ) | `src/streaming/consumer/bronze_consumer.py` | ✅ |
| Logging estruturado JSON Lines | `src/streaming/logging/` | ✅ |
| Storage abstrato (MinIO + DryRun) | `src/streaming/storage/` | ✅ |
| Buckets S3 via Terraform (bronze, silver, gold) | `infra/terraform/` | ✅ |
| 35 testes unitários | `src/streaming/tests/` | ✅ |
| Documentação: README + ADRs + Modelagem | `README.md`, `docs/` | ✅ |

### E2 - Pipeline (Completo)

| Componente | Localização | Status |
|---|---|---|
| 9 ETLs Bronze (8 PostgreSQL + 1 MongoDB) | `src/processamento/bronze/` | ✅ |
| Classe base `AbstractETL` (Template Method) | `src/processamento/abstract_etl.py` | ✅ |
| 10 ETLs Silver com deduplicação e tipagem | `src/processamento/silver/` | ✅ |
| Classe base `SilverETL` | `src/processamento/silver/base_silver_etl.py` | ✅ |
| 7 ETLs Gold (5 dimensões + 2 fatos) | `src/processamento/gold/` | ✅ |
| Star schema dimensional | `src/processamento/gold/` | ✅ |
| DAG Airflow `pipeline_iot` (@daily) | `src/dags/pipeline_iot.py` | ✅ |
| Ingestão batch do PostgreSQL via Airflow | DAG Bronze tasks | ✅ |
| Validação de qualidade PyDeequ | Integrado ao `AbstractETL` | ✅ |
| Delta Lake como formato de tabela Silver/Gold | Configurado no Spark | ✅ |
| Spark Standalone (Master + Worker) | `infra/docker/spark/` | ✅ |
| Dockerfile Spark customizado (Python 3.11) | `infra/docker/spark/Dockerfile` | ✅ |
| Observabilidade com Grafana + Loki + Promtail | `infra/docker/docker-compose.yml` | ✅ |
| Catálogo de dados documentado | `docs/catalogo-dados.md` | ✅ |
| ADRs completos (9 decisões) | `docs/arquitetura.md` | ✅ |

### Infraestrutura Adicional

| Componente | Localização | Status |
|---|---|---|
| Airflow CeleryExecutor com Dockerfile customizado | `infra/airflow/` | ✅ |
| Hive Metastore + Trino | `infra/docker/docker-compose.yml` | ✅ |
| Scripts de automação (start/stop) | `scripts/` | ✅ |
| Terraform para IaC | `infra/terraform/` | ✅ |

---

## Pendências para E3 - Integração Final

### 1. Pipeline Streaming em Near Real-Time (4 pts)

Pipeline processando eventos Kafka em janelas de 1-5 minutos (Spark Structured Streaming ou Flink).

| Item | Descrição | Prioridade |
|---|---|---|
| Job Spark Streaming | Processar eventos Kafka com janelas temporais (tumbling/sliding) | Alta |
| Watermarks | Gerenciar eventos atrasados (late data handling) | Média |
| Escrita contínua | Gravar resultados em near real-time no MinIO/Delta | Alta |

### 2. Detecção de Anomalias com Alertas (3 pts)

Sistema de alertas para leituras fora do range esperado do equipamento.

| Item | Descrição | Prioridade |
|---|---|---|
| Detecção baseada em range | Comparar valores com `sensors.range` do MongoDB | Alta |
| Sistema de notificação | Alertas via log, webhook ou e-mail | Média |
| Histórico de alertas | Persistir alertas detectados para auditoria | Média |

### 3. Integração com API de Manutenção (2 pts)

Ingestão de dados de uma API REST de manutenção via Airflow.

| Item | Descrição | Prioridade |
|---|---|---|
| API mock ou FastAPI | Endpoint REST simulando dados de manutenção | Média |
| DAG de ingestão | Task Airflow consumindo a API periodicamente | Média |

### 4. Testes de Qualidade de Dados (2 pts)

Mínimo de 5 assertions de qualidade de dados.

| Item | Descrição | Prioridade |
|---|---|---|
| Testes PyDeequ adicionais | Completude, unicidade, range, referencial | Média |
| Relatório de execução | Output dos testes em formato auditável | Baixa |

### 5. Dashboard (Recomendado)

Visualizações analíticas sobre os dados Gold.

| Item | Descrição | Prioridade |
|---|---|---|
| Metabase ou Superset | Conectar ao Trino/MinIO para ler dados Gold | Média |
| 3+ visualizações | Métricas por fábrica, anomalias, série temporal | Média |

### 6. Apresentação Final (4 pts)

| Item | Descrição | Prioridade |
|---|---|---|
| Roteiro de demo ao vivo (15-20 min) | Subir ambiente, mostrar fluxo, provocar anomalia | Alta |
| Vídeo backup (3-5 min) | Gravação para caso a demo falhe | Média |

---

## Itens Opcionais (Pontos Extras)

| Item | Pontuação | Status |
|---|---|---|
| Delta Lake como Table Format | +2 pts | ✅ Implementado |
| Terraform como IaC | +1 pt | ✅ Implementado |
| Great Expectations / PyDeequ | +1 pt | ✅ Implementado (PyDeequ) |
| CI/CD (GitHub Actions) | +1 pt | Pendente |
| dbt para transformações | +1 pt | Pendente |
