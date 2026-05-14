## Consumer Bronze — Documentação Técnica

Documentação do consumer Kafka responsável pela camada Bronze da arquitetura Medallion.

---

## Arquitetura (SOLID)

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

| Princípio | Implementação |
|---|---|
| **SRP** | Cada classe tem uma única responsabilidade (Buffer, Validador, Processor, Storage) |
| **OCP** | `StorageClient` como Protocol permite adicionar backends (S3, GCS) sem alterar o consumer |
| **LSP** | `BaseConsumer` pode ser herdado por Silver/Gold mantendo comportamento consistente |
| **ISP** | Interfaces específicas por camada — sem interface monolítica |
| **DIP** | Consumer depende de abstrações (`StorageClient` Protocol), não de implementações concretas |

---

## Particionamento no MinIO (Camada Bronze)

Os arquivos são gravados em **JSON Lines (.ndjson)** com particionamento Hive-style, compatível com leitura nativa pelo Apache Spark:

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

Leitura com Spark:

```python
df = spark.read.json("s3a://bronze/")
df.filter("factory_id = 'FAB-SP-01' AND dt = '2025-03-29'")
```

Cada evento é enriquecido com metadados de ingestão:

| Campo | Origem |
|---|---|
| `_ingested_at` | UTC do momento da gravação |
| `_kafka_offset` | offset do consumer |
| `_kafka_partition` | partição do tópico |
| `_kafka_topic` | nome do tópico |
| `_kafka_key` | key da mensagem |

---

## Logs Estruturados

O consumer emite logs em **JSON Lines** — um objeto por linha, compatível com Elasticsearch, Loki e CloudWatch sem parsing adicional.

**Gravação bem-sucedida:**

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

**Anomalia detectada:**

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

**Estatísticas parciais (a cada flush):**

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

---

## Parâmetros do Consumer Bronze

Todas as configurações podem ser definidas via `.env` ou argumentos CLI (CLI sobrescreve `.env`):

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
| `--dry-run` | — | `false` | Consome e valida sem gravar no MinIO |
| `--log-level` | — | `INFO` | Nível de log (DEBUG, INFO, WARNING, ERROR) |

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

---

## Decisões Técnicas

**Commit manual de offset:** o consumer só confirma o offset após gravar com sucesso no MinIO. Se o processo cair no meio, as mensagens são reprocessadas — garantindo at-least-once delivery sem perda de dados.

**Chave de particionamento por `equipment_id`:** garante que eventos do mesmo equipamento sempre vão para a mesma partição Kafka, preservando a ordem cronológica necessária para análise de séries temporais no Silver.

**Dead-letter queue:** eventos com schema inválido são isolados em `bronze/_dead_letter/` com o motivo do erro registrado em `_dlq_reason`, sem travar o pipeline principal.

**Fallback de validação:** se o Schema Registry estiver offline, o consumer usa o schema local como fallback automático — o pipeline nunca para por indisponibilidade do Registry.

**Testes sem dependências externas:** 35 testes unitários cobrem todas as camadas usando mocks para Kafka e Storage. É possível rodar `python -m unittest discover` sem ter Kafka ou MinIO rodando.
