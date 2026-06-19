# Gaps e Pendências do Projeto IoT Lakehouse

> Documento gerado em 2026-05-07 com base em análise completa do repositório.
> Referência de roadmap: README.md (linhas 567-575).

---

## O que está implementado (E1 - Completo)

| Componente | Localização | Status |
|---|---|---|
| Simulador de sensores IoT | `src/ingestao/sensor_simulator.py` | ✅ |
| Consumer Bronze (Kafka -> MinIO) | `src/streaming/consumer/` | ✅ |
| Buffer particionado Hive-style | `src/streaming/buffer/` | ✅ |
| Validação de schema (Registry + fallback local) | `src/streaming/schema/` + `validation/` | ✅ |
| Dead-letter queue (DLQ) | `bronze_consumer.py` | ✅ |
| Logging estruturado JSON Lines | `src/streaming/logging/` | ✅ |
| Storage abstrato (MinIO + DryRun) | `src/streaming/storage/` | ✅ |
| Infraestrutura Docker (Kafka, Zookeeper, Schema Registry, MinIO, Kafka UI) | `docker-compose_tulio.yml` | ✅ |
| Buckets S3 via Terraform (bronze/silver/gold) | `infra/terraform/` | ✅ |
| 35 testes unitários | `src/streaming/tests/` | ✅ |
| Documentação de arquitetura (ADR) | `docs/arquitetura.md` | ✅ |
| Modelagem de dados MongoDB | `docs/modelo-dados.md` | ✅ |

---

## O que NÃO temos

### 1. Camada Silver - Processamento (E2, alta prioridade)

A camada Silver é o próximo passo crítico da arquitetura Medallion. Os dados brutos no MinIO (`bronze/`) precisam ser limpos, deduplicados e tipados para uso analítico.

**O que falta construir:**

| Item | Descrição |
|---|---|
| Consumer/Job Silver | Lê arquivos `.ndjson` da partição Bronze e aplica transformações |
| Deduplicação | Remove eventos com mesmo `event_id` (at-least-once pode gerar duplicatas no Kafka) |
| Tipagem e cast | Converte campos para tipos corretos (e.g., `timestamp` -> datetime, `value` -> float) |
| Enriquecimento com MongoDB | Consulta cadastro de equipamentos para adicionar `equipment_name`, `factory_name`, faixa de operação do sensor |
| Detecção de anomalias baseada em range | Usa `sensors.range` do MongoDB para sinalizar leituras fora do normal (hoje `is_anomaly` vem do simulador, não calculado) |
| Escrita no Silver | Grava em `silver/` no MinIO em formato Parquet ou NDJSON particionado |
| Schema Silver | Define e documenta o schema de saída da camada Silver |

**Dependência:** MongoDB precisa estar populado com o cadastro de equipamentos (a modelagem já está documentada em `docs/modelo-dados.md`, mas nenhum código faz CRUD ou leitura no MongoDB).

---

### 2. Camada Gold - Agregações (E2/E3)

Camada de consumo analítico. Alimenta dashboards e relatórios.

**O que falta construir:**

| Item | Descrição |
|---|---|
| Job de agregação horária | Agrupa leituras por `(factory_id, measurement_type, hour)` - média, min, max, p95 |
| Job de agregação diária | Sumariza métricas diárias por equipamento |
| Detecção de anomalias agregadas | % de leituras anômalas por hora/dia por sensor |
| Escrita no Gold | Grava em `gold/` no MinIO (Parquet ideal para BI) |
| Schema Gold | Define e documenta o schema de saída |

---

### 3. Integração com MongoDB (não iniciada)

O documento de modelagem está feito (`docs/modelo-dados.md`), mas nenhum código usa o MongoDB.

**O que falta:**

| Item | Descrição |
|---|---|
| Script de seed/fixture | Popular MongoDB com dados de equipamentos e sensores para dev/teste |
| Cliente MongoDB em Python | Módulo `src/storage/mongo_client.py` ou similar para leitura do cadastro |
| Integração no Silver | Usar o cadastro para enriquecer eventos (join por `sensor_id` / `equipment_id`) |
| Inicialização real do container | `infra/docker/mongo-init/init-script.js` atual cria apenas uma coleção "teste" sem dados reais |
| Variáveis .env para MongoDB | `.env.template` tem `MONGO_USER`/`MONGO_PASS` mas não estão integradas no código Python |

---

### 4. Observabilidade (E3)

Nenhuma métrica de runtime é exportada. O sistema produz logs estruturados, mas sem coleta ou visualização.

**O que falta:**

| Item | Descrição |
|---|---|
| Prometheus exporter | Exportar contadores do consumer (received, written, invalid, anomalies) como métricas |
| Grafana dashboards | Dashboard de throughput, taxa de erro, tamanho de DLQ, latência de flush |
| Loki (log aggregation) | Coletar os JSON Lines do consumer para busca centralizada |
| Alertas no Prometheus/Grafana | Disparar alerta quando DLQ > threshold ou consumer parar |
| Health check endpoint | Endpoint HTTP `/health` ou `/metrics` no consumer |
| Docker Compose para observabilidade | Stack Prometheus + Grafana + Loki ainda não existe (branch `feat/docker-observability` existe mas não foi mergeada) |

---

### 5. CI/CD (E3)

Nenhum pipeline de automação existe.

**O que falta:**

| Item | Descrição |
|---|---|
| `.github/workflows/ci.yml` | Rodar `ruff check` + `pytest` em cada PR |
| `.github/workflows/build.yml` | Build e push de imagens Docker |
| Linting gate | Bloquear merge se `ruff` encontrar erros |
| Test coverage report | Integrar `pytest --cov` e publicar cobertura |
| Workflow de release | Tag + Changelog automático |

---

### 6. Runbook Operacional (documentação)

O arquivo `docs/runbook.md` está **vazio**.

**O que falta documentar:**

| Item |
|---|
| Como subir o ambiente local (`docker compose up`, ordem dos serviços) |
| Como rodar o simulador e o consumer |
| Como verificar se dados chegaram ao MinIO |
| Como consultar a DLQ e reprocessar eventos inválidos |
| Como escalar o consumer (múltiplas instâncias, partições Kafka) |
| Troubleshooting: Kafka não sobe, MinIO indisponível, Schema Registry offline |
| Checklist de deploy em produção |

---

### 7. Segurança para produção (não crítico para E1, mas relevante)

**Gaps identificados:**

| Item | Situação atual |
|---|---|
| Kafka sem autenticação/TLS | PLAINTEXT listener - adequado apenas para dev local |
| MinIO com credenciais padrão | `minioadmin:minioadmin` hardcoded no `docker-compose_tulio.yml` |
| Schema Registry sem auth | Acesso aberto na porta 8081 |
| Variáveis sensíveis sem rotação | Nenhum mecanismo de secrets (Vault, AWS Secrets Manager) |

---

### 8. Outros gaps menores

| Item | Descrição |
|---|---|
| `pyproject.toml` sem dependências declaradas | As dependências Python (kafka-python, boto3, requests, python-dotenv) não estão listadas explicitamente no `[tool.poetry.dependencies]` |
| `docker-compose_tulio.yml` com nome não-padrão | Nome do arquivo deveria ser `docker-compose.yml` ou `docker-compose.e1.yml` para facilitar uso com `docker compose up` sem `-f` |
| Infraestrutura Docker fragmentada | Existe `docker-compose_tulio.yml` (E1 stack completa) e `infra/docker/docker-compose.yml` (legacy MongoDB/Postgres) - pode gerar confusão |
| Schema Evolution | Não há suporte a múltiplas versões de schema (breaking changes no formato de evento quebraria o consumer) |
| Testes de integração | Apenas testes unitários com mocks - sem testes rodando contra Kafka/MinIO reais |

---

## Resumo por prioridade

| Prioridade | Item | Esforço estimado |
|---|---|---|
| 🔴 Alta | Camada Silver (deduplicação + enriquecimento) | Grande |
| 🔴 Alta | Seed de dados no MongoDB | Pequeno |
| 🟠 Média | Camada Gold (agregações) | Médio |
| 🟠 Média | CI/CD (GitHub Actions) | Pequeno |
| 🟠 Média | Runbook operacional | Pequeno |
| 🟡 Baixa | Observabilidade (Prometheus + Grafana) | Médio |
| 🟡 Baixa | Dependências explícitas no pyproject.toml | Muito pequeno |
| 🟡 Baixa | Segurança para produção | Grande |
| 🟡 Baixa | Testes de integração | Médio |
| 🟡 Baixa | Schema Evolution | Grande |
