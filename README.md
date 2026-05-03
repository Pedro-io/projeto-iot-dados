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
| Bibliotecas Python | kafka-python, boto3, requests, python-dotenv | latest |
| Gerenciamento de Dependências | Poetry | 1.x |

---

## Estrutura do Repositório

```
projeto-iot-dados/
├── .env                            # Credenciais e configurações (não versionado)
├── .env.template                   # Template de variáveis de ambiente
├── .gitignore                      # Arquivos ignorados pelo Git
├── docker-compose.yml              # Infraestrutura completa
├── README.md                       # Este arquivo
├── pyproject.toml                  # Gerenciamento Poetry
├── docs/
│   ├── modelo-dados.md             # Setup + ADRs de modelagem 
│   └── arquitetura.md              # Diagrama e decisões arquiteturais
├── infra/
│   ├── docker/                     # Configurações Docker
│   └── terraform/                  # Provisionamento de recursos
└── src/
    ├── ingestao/
    │   └── sensor_simulator.py     # Produtor Kafka — simula sensores IoT
    └── streaming/
        ├── __init__.py
        ├── config.py               # Configurações centralizadas (lê .env)
        ├── buffer/
        │   └── partitioned_buffer.py    # Buffer particionado Hive-style
        ├── consumer/
        │   ├── base_consumer.py         # Loop genérico Kafka (ABC)
        │   └── bronze_consumer.py       # Consumer Bronze + entrypoint
        ├── logging/
        │   └── json_formatter.py        # Logs estruturados JSON Lines
        ├── process/
        │   └── bronze_process.py        # Validação + enriquecimento
        ├── schema/
        │   ├── local_schema.py          # Fallback schema local
        │   └── schema_registry_client.py # Cliente Schema Registry
        ├── storage/
        │   ├── base_storage.py          # Protocol StorageClient
        │   ├── dryrun_client.py         # Mock para testes/dev
        │   └── minio_client.py          # Cliente MinIO/S3
        └── validation/
            └── schema_validator.py      # Validação de eventos
```

---

## Arquitetura do Consumer Bronze (SOLID)

O consumer foi desenvolvido seguindo os princípios **SOLID**, com separação clara de responsabilidades:

```
                  ┌────────────────────────────────────────┐
                  │      BronzeConsumer (entrypoint)       │
                  │  ┌──────────────────────────────────┐  │
Kafka ──► consumer├──┤  BaseConsumer (loop, commit, DLQ)│  │
                  │  └──────────────────────────────────┘  │
                  │             │                          │
                  │   ┌─────────┴──────────┐               │
                  │   ▼                    ▼               │
                  │ BronzeProcessor    PartitionedBuffer   │
                  │   │                    │               │
                  │   ▼                    ▼               │
                  │ SchemaValidator    StorageClient ──► MinIO / DryRun
                  │   │                                    │
                  │   ▼                                    │
                  │ SchemaRegistryClient                   │
                  └────────────────────────────────────────┘
```

**Princípios aplicados:**

| Princípio | Implementação |
|---|---|
| **SRP** | Cada classe tem uma única responsabilidade (BufferParticionado, Validador, Processor, Storage) |
| **OCP** | `StorageClient` como Protocol permite adicionar backends (S3, GCS) sem alterar o consumer |
| **LSP** | `BaseConsumer` pode ser herdado por Silver/Gold mantendo comportamento consistente |
| **ISP** | Interfaces específicas por camada (não há interface monolítica) |
| **DIP** | Consumer depende de abstrações (`StorageClient` Protocol), não de implementações concretas |

---

## Serviços e Portas

| Serviço | URL | Descrição |
|---|---|---|
| Kafka Broker | `localhost:9092` | Recebe eventos dos sensores |
| Schema Registry | `http://localhost:8081` | Valida e versiona schemas |
| Kafka UI | `http://localhost:8080` | Interface web para tópicos e schemas |
| MinIO S3 API | `http://localhost:9000` | API de armazenamento |
| MinIO Console | `http://localhost:9001` | Interface web do lakehouse |

---

## Pré-requisitos

| Ferramenta | Versão mínima | Link |
|---|---|---|
| Docker Desktop | 24.x | docker.com/products/docker-desktop |
| Python | 3.10+ | python.org/downloads |
| Poetry | 1.x | python-poetry.org |
| Git | 2.x | git-scm.com/downloads |

> **Windows:** ao instalar o Python, marque obrigatoriamente **"Add Python to PATH"** antes de clicar em Install Now.

---

## Instalação e Execução

### 1. Clonar o repositório

```bash
git clone https://github.com/equipe/projeto-iot-dados.git
cd projeto-iot-dados
```

### 2. Configurar variáveis de ambiente

Copie o template e preencha com suas credenciais:

```bash
cp .env.template .env
```

Edite `.env` com seus valores reais:

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

> **IMPORTANTE:** O arquivo `.env` contém credenciais sensíveis e está no `.gitignore`. Nunca comite credenciais no Git.

### 3. Configurar ambiente Python com Poetry

Configure o Poetry para criar ambiente virtual dentro do projeto:

```bash
poetry config virtualenvs.in-project true
```

Instale as dependências:

```bash
poetry install
```

**Alternativa sem Poetry (pip direto):**

```bash
pip install kafka-python boto3 requests python-dotenv
```

### 4. Subir a infraestrutura (Docker)

Com o Docker Desktop aberto e em execução:

```bash
docker-compose --env-file .env -f infra/docker/docker-compose.yml up -d
```

Ou, se o `docker-compose.yml` estiver na raiz:

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

### 5. Verificar o Schema Registry

Acesse no browser:

```
http://localhost:8081/subjects
```

Resposta esperada: `["iot-sensors-raw-value"]` — confirma que o schema do tópico foi registrado automaticamente pelo `schema-registry-init`.

### 6. Rodar o simulador de sensores

```bash
cd src/ingestao
poetry run python sensor_simulator.py --events-per-second 50 --anomaly-rate 0.1
```

**Sem Poetry:**

```bash
cd src/ingestao
python sensor_simulator.py --events-per-second 50 --anomaly-rate 0.1
```

### 7. Rodar o consumer Bronze em outro terminal

**Com Poetry:**

```bash
cd src
poetry run python -m streaming.consumer.bronze_consumer
```

**Sem Poetry:**

```bash
cd src
python -m streaming.consumer.bronze_consumer
```

**Modo dry-run (recomendado para testes):**

```bash
cd src
python -m streaming.consumer.bronze_consumer --dry-run
```

O consumer conecta automaticamente ao Schema Registry em `http://localhost:8081` e valida cada evento antes de gravar. Se o Schema Registry estiver offline, o consumer usa validação local como fallback sem interromper o pipeline.

### 8. Verificar os dados

| Interface | URL | O que verificar |
|---|---|---|
| Kafka UI | http://localhost:8080 | Topics → iot-sensors-raw → Messages |
| Schema Registry | http://localhost:8081/subjects | Schema registrado |
| MinIO Console | http://localhost:9001 | Bucket bronze → pastas factory_id=... |

---

## Testes Unitários

O projeto inclui **35 testes unitários** cobrindo todas as camadas:

```bash
cd projeto-iot-dados
PYTHONPATH=src python -m unittest discover -s tests -v
```

**Com Poetry:**

```bash
poetry run python -m unittest discover -s tests -v
```

| Suíte | Cobre |
|---|---|
| `test_logging.py` | JsonFormatter, setup_logging |
| `test_buffer.py` | PartitionedBuffer (flush por size/interval, drain) |
| `test_validation.py` | SchemaValidator com fallback local |
| `test_storage.py` | DryRunClient, contrato StorageClient |
| `test_processor.py` | BronzeProcessor (enriquecimento + DLQ) |
| `test_consumer.py` | BronzeConsumer end-to-end com Kafka mockado |

**Cobertura:** 100% dos módulos principais sem dependência de Kafka/MinIO real.

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

Cada evento gravado é enriquecido com metadados (prefixo `_`):

| Campo | Origem |
|---|---|
| `_ingested_at` | UTC do momento da gravação |
| `_kafka_offset` | offset do consumer |
| `_kafka_partition` | partição do tópico |
| `_kafka_topic` | nome do tópico |
| `_kafka_key` | key da mensagem |

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

Todas as configurações podem ser definidas via **variáveis de ambiente** (`.env`) ou **argumentos CLI** (que sobrescrevem o `.env`):

| Parâmetro CLI | Variável .env | Padrão | Descrição |
|---|---|---|---|
| `--bootstrap-servers` | `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Endereço do Kafka |
| `--topic` | `KAFKA_TOPIC` | `iot-sensors-raw` | Tópico a consumir |
| `--group-id` | `KAFKA_GROUP_ID` | `bronze-writer` | Consumer group ID |
| `--minio-endpoint` | `MINIO_ENDPOINT` | `http://localhost:9000` | Endpoint do MinIO |
| `--minio-access-key` | `MINIO_ACCESS_KEY` | `minioadmin` | Access key MinIO |
| `--minio-secret-key` | `MINIO_SECRET_KEY` | `minioadmin` | Secret key MinIO |
| `--bucket` | `DEFAULT_BUCKET` | `bronze` | Bucket de destino |
| `--flush-size` | `DEFAULT_FLUSH_SIZE` | `100` | Eventos por arquivo |
| `--flush-interval` | `DEFAULT_FLUSH_INTERVAL` | `30` | Segundos máximos entre gravações |
| `--schema-registry-url` | `DEFAULT_SCHEMA_REGISTRY_URL` | `http://localhost:8081` | URL do Schema Registry |
| `--dry-run` | - | `false` | Consome e valida sem gravar no MinIO |
| `--log-level` | - | `INFO` | Nível de log (DEBUG, INFO, WARNING, ERROR) |

**Exemplo combinando .env + CLI:**

```bash
# .env define MINIO_ENDPOINT=http://minio-prod:9000
# CLI sobrescreve para localhost temporariamente
python -m streaming.consumer.bronze_consumer --minio-endpoint http://localhost:9000
```

---

## Decisões Técnicas Relevantes

### Arquitetura SOLID

**Refatoração completa com princípios SOLID:** o consumer foi decomposto em camadas com responsabilidades únicas. `BaseConsumer` implementa o loop genérico Kafka e pode ser herdado por Silver/Gold sem duplicação de código. `StorageClient` é um Protocol (Strategy Pattern) — trocar MinIO por S3, GCS ou DryRun é uma única linha de injeção de dependência. `BronzeProcessor` encapsula toda a lógica de validação e enriquecimento, isolada do transporte (Kafka) e da persistência (MinIO). Isso permite testar cada camada isoladamente com 100% de cobertura.

**Schema Registry para validação formal:** o schema JSON do tópico `iot-sensors-raw` é registrado automaticamente na inicialização via `schema-registry-init`. O consumer consulta o Registry para validar campos obrigatórios e enums em tempo real. Em caso de falha do Registry, o consumer usa validação local como fallback — garantindo resiliência do pipeline.

**Configuração via .env:** todas as credenciais e configurações sensíveis são lidas de variáveis de ambiente através do arquivo `.env`, nunca hardcoded. O arquivo `config.py` usa `os.getenv()` com fallbacks seguros. O `.env` está no `.gitignore` para evitar vazamento de credenciais. Um `.env.template` documenta todas as variáveis necessárias.

**Commit manual de offset:** o consumer só confirma o offset após gravar com sucesso no MinIO. Se o processo cair no meio, as mensagens são reprocessadas — garantindo at-least-once delivery sem perda de dados.

**Chave de particionamento por `equipment_id`:** garante que eventos do mesmo equipamento sempre vão para a mesma partição Kafka, preservando a ordem cronológica necessária para análise de séries temporais no Silver.

**Dead-letter queue:** eventos com schema inválido são isolados em `bronze/_dead_letter/` com o motivo do erro registrado no campo `_dlq_reason`, facilitando auditoria e reprocessamento futuro sem travar o pipeline principal.

**Logs JSON estruturados:** todos os eventos de log incluem campos contextuais (`sensor_id`, `factory_id`, `events`, `bytes`, `rate_per_s`) além da mensagem, permitindo filtros e dashboards sem parsing de texto livre. Implementado via `JsonFormatter` customizado compatível com stacks de observabilidade modernas.

**Rotação de logs dos containers:** todos os serviços têm `logging.driver: json-file` com `max-size` e `max-file` configurados, evitando crescimento ilimitado dos logs em disco no ambiente local.

**Testes sem dependências externas:** 35 testes unitários cobrem todas as camadas usando mocks para Kafka e Storage. É possível rodar `python -m unittest discover` sem ter Kafka ou MinIO rodando, acelerando o ciclo de desenvolvimento e CI/CD.

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
cd src
python -m streaming.consumer.bronze_consumer --dry-run --flush-size 10 --flush-interval 5

# Rodar testes unitários
PYTHONPATH=src python -m unittest discover -s tests -v

# Rodar linter (com Poetry)
poetry run ruff check .
poetry run ruff format .

# Verificar configurações carregadas do .env
cd src
python -c "from streaming.config import DEFAULT_MINIO_ENDPOINT, DEFAULT_BUCKET; print(f'MinIO: {DEFAULT_MINIO_ENDPOINT}'); print(f'Bucket: {DEFAULT_BUCKET}')"
```

---

## Solução de Problemas

| Erro | Causa | Solução |
|---|---|---|
| `kafka is unhealthy` | Kafka ainda inicializando | Aguarde 2 minutos e rode `docker compose ps` |
| `NoBrokersAvailable` | Kafka não acessível | Verifique se o Docker está aberto e o Kafka está `Up` |
| `ERRO: Variável 'KAFKA_BOOTSTRAP_SERVERS' não definida` | Arquivo `.env` não existe ou está incompleto | Copie `.env.template` para `.env` e preencha |
| `python-dotenv não instalado` | Biblioteca faltando | Execute `pip install python-dotenv` |
| `docker não reconhecido` | Docker Desktop fechado | Abra o Docker Desktop e aguarde a baleia estabilizar |
| Schema Registry retorna `404` | Schema ainda não registrado ou Registry offline | Consumer cai automaticamente para validação local |
| Bucket bronze vazio | Consumer rodou com `--dry-run` | Rode sem a flag `--dry-run` |
| VS Code não reconhece `docker` ou `pip` | Ferramentas instaladas com VS Code aberto | Feche e reabra o VS Code após instalar |
| Testes falhando com `ImportError` | PYTHONPATH não configurado | Execute `PYTHONPATH=src python -m unittest discover` |

---

## Próximos Passos (Roadmap)

- [ ] **E2 — Camada Silver:** Deduplicação, limpeza e tipagem via PySpark
- [ ] **E2 — Camada Gold:** Agregações horárias/diárias para dashboards
- [ ] **E3 — Observabilidade:** Exportar métricas para Prometheus + Grafana
- [ ] **E3 — Alerting:** Integração com PagerDuty/Slack quando DLQ ultrapassar threshold
- [ ] **E3 — CI/CD:** GitHub Actions com linting + testes + build de imagem Docker
- [ ] **E4 — Schema Evolution:** Suporte a múltiplas versões de schema com compatibilidade

---

## Integridade Acadêmica

Uso de IA generativa como ferramenta de apoio ao desenvolvimento. O entendimento do código, das decisões de arquitetura e dos princípios SOLID aplicados é responsabilidade da equipe conforme política da disciplina.

---

## Equipe

| Nome | Matrícula | Responsabilidade |
|---|---|---|
| - | - | Arquitetura e camada Bronze |
| - | - | Infraestrutura Docker/Terraform |
| - | - | Testes e documentação |

---

## Licença

Este projeto é desenvolvido para fins acadêmicos — PUC Minas 2026/1.
