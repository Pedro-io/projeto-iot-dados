# Catálogo de Dados - Plataforma IoT Lakehouse

**Última atualização:** 2026-06-19  
**Owner:** pedro.rodrigues@bdtech.ai  
**Ambiente de referência:** `prd` (buckets `bronze`, `silver`, `gold`)

---

## Índice

1. [Camada Bronze](#1-camada-bronze)
   - [bronze/postgres/fabricas](#11-bronzepostgresfabricas)
   - [bronze/postgres/equipamentos](#12-bronzepostgresequipamentos)
   - [bronze/postgres/tipos_equipamento](#13-bronzepostgrestipos_equipamento)
   - [bronze/postgres/sensores](#14-bronzepostgressensores)
   - [bronze/postgres/tipos_medicao](#15-bronzepostgrestipos_medicao)
   - [bronze/postgres/leituras](#16-bronzepostgresleituras)
   - [bronze/postgres/metadados_leitura](#17-bronzepostgresmetadados_leitura)
   - [bronze/postgres/status_qualidade](#18-bronzepostgresstatus_qualidade)
   - [bronze/mongo/equipamentos](#19-bronzemongoquipamentos)
   - [bronze/factory_id=\*/… (Kafka)](#110-bronzefactory_idmeasurement_typedt-kafka)
2. [Camada Silver](#2-camada-silver)
   - [silver/postgres/fabricas](#21-silverpostgresfabricas)
   - [silver/postgres/equipamentos](#22-silverpostgresequipamentos)
   - [silver/postgres/tipos_equipamento](#23-silverpostgrestipos_equipamento)
   - [silver/postgres/sensores](#24-silverpostgressensores)
   - [silver/postgres/tipos_medicao](#25-silverpostgrestipos_medicao)
   - [silver/postgres/leituras](#26-silverpostgresleituras)
   - [silver/postgres/metadados_leitura](#27-silverpostgresmetadados_leitura)
   - [silver/postgres/status_qualidade](#28-silverpostgresstatus_qualidade)
   - [silver/mongo/equipamentos](#29-silvermongoquipamentos)
   - [silver/kafka/sensor_events](#210-silverkafkasensor_events)
3. [Camada Gold - Modelagem Dimensional Proposta](#3-camada-gold--modelagem-dimensional-proposta)
   - [Star Schema](#31-star-schema)
   - [dim_tempo](#32-dim_tempo)
   - [dim_fabrica](#33-dim_fabrica)
   - [dim_equipamento](#34-dim_equipamento)
   - [dim_sensor](#35-dim_sensor)
   - [dim_tipo_medicao](#36-dim_tipo_medicao)
   - [fct_leituras_hora](#37-fct_leituras_hora)
   - [fct_anomalias_dia](#38-fct_anomalias_dia)
4. [Linhagem de Dados](#4-linhagem-de-dados)
5. [Convenções e Metadados Técnicos](#5-convenções-e-metadados-técnicos)

---

## 1. Camada Bronze

**Formato:** Parquet (PostgreSQL e MongoDB) · NDJSON (Kafka)  
**Modo de escrita:** `append` - dados brutos imutáveis, nunca sobrescritos  
**Localização:** `s3a://bronze/{source}/{table}/`

### 1.1 bronze/postgres/fabricas

**Origem:** `erp_legado.fabricas` (PostgreSQL)  
**Carga:** Full load diário  
**ETL:** `src/processamento/bronze/postgres/fabricas.py`

| Coluna           | Tipo Spark    | Tipo PostgreSQL  | Descrição                               | Obrigatório |
|------------------|---------------|------------------|-----------------------------------------|-------------|
| `id`             | StringType    | VARCHAR(20)      | Identificador da fábrica (ex: FAB-SP-01)| Sim         |
| `nome`           | StringType    | TEXT             | Nome da fábrica                         | Sim         |
| `latitude`       | DoubleType    | NUMERIC(9,6)     | Latitude geográfica                     | Sim         |
| `longitude`      | DoubleType    | NUMERIC(9,6)     | Longitude geográfica                    | Sim         |
| `_execution_date`| DateType      | -                | Data de execução do job                 | Sim         |
| `_processed_at`  | TimestampType | -                | Timestamp de processamento              | Sim         |
| `_data_source`   | StringType    | -                | Valor fixo: `"postgres"`               | Sim         |

---

### 1.2 bronze/postgres/equipamentos

**Origem:** `erp_legado.equipamentos`  
**Carga:** Full load diário  
**ETL:** `src/processamento/bronze/postgres/equipamentos.py`

| Coluna               | Tipo Spark    | Tipo PostgreSQL  | Descrição                                          | Obrigatório |
|----------------------|---------------|------------------|----------------------------------------------------|-------------|
| `id`                 | StringType    | VARCHAR(20)      | Identificador do equipamento (ex: EQ-1234)         | Sim         |
| `nome`               | StringType    | TEXT             | Nome do equipamento                                | Sim         |
| `tipo_equipamento_id`| IntegerType   | INTEGER          | FK → tipos_equipamento.id                          | Sim         |
| `fabrica_id`         | StringType    | VARCHAR(20)      | FK → fabricas.id                                   | Sim         |
| `status`             | StringType    | VARCHAR(20)      | Status: `ativo`, `inativo`, `manutencao`           | Sim         |
| `data_instalacao`    | DateType      | DATE             | Data de instalação do equipamento                  | Não         |
| `_execution_date`    | DateType      | -                | Data de execução do job                            | Sim         |
| `_processed_at`      | TimestampType | -                | Timestamp de processamento                         | Sim         |
| `_data_source`       | StringType    | -                | Valor fixo: `"postgres"`                          | Sim         |

---

### 1.3 bronze/postgres/tipos_equipamento

**Origem:** `erp_legado.tipos_equipamento`  
**Carga:** Full load diário  
**ETL:** `src/processamento/bronze/postgres/tipos_equipamento.py`

| Coluna           | Tipo Spark    | Tipo PostgreSQL | Descrição                        | Obrigatório |
|------------------|---------------|-----------------|----------------------------------|-------------|
| `id`             | IntegerType   | SERIAL          | PK da tabela de tipos            | Sim         |
| `nome`           | StringType    | TEXT            | Nome do tipo (ex: "compressor")  | Sim         |
| `_execution_date`| DateType      | -               | Data de execução do job          | Sim         |
| `_processed_at`  | TimestampType | -               | Timestamp de processamento       | Sim         |
| `_data_source`   | StringType    | -               | Valor fixo: `"postgres"`        | Sim         |

---

### 1.4 bronze/postgres/sensores

**Origem:** `erp_legado.sensores`  
**Carga:** Full load diário  
**ETL:** `src/processamento/bronze/postgres/sensores.py`

| Coluna            | Tipo Spark    | Tipo PostgreSQL | Descrição                                           | Obrigatório |
|-------------------|---------------|-----------------|-----------------------------------------------------|-------------|
| `id`              | StringType    | VARCHAR(20)     | Identificador do sensor (ex: SENS-001-TEMP)         | Sim         |
| `equipamento_id`  | StringType    | VARCHAR(20)     | FK → equipamentos.id                                | Sim         |
| `tipo_medicao_id` | IntegerType   | INTEGER         | FK → tipos_medicao.id                               | Sim         |
| `status`          | StringType    | VARCHAR(20)     | Status: `ativo`, `inativo`, `manutencao`            | Sim         |
| `data_instalacao` | DateType      | DATE            | Data de instalação do sensor                        | Não         |
| `_execution_date` | DateType      | -               | Data de execução do job                             | Sim         |
| `_processed_at`   | TimestampType | -               | Timestamp de processamento                          | Sim         |
| `_data_source`    | StringType    | -               | Valor fixo: `"postgres"`                           | Sim         |

---

### 1.5 bronze/postgres/tipos_medicao

**Origem:** `erp_legado.tipos_medicao`  
**Carga:** Full load diário  
**ETL:** `src/processamento/bronze/postgres/tipos_medicao.py`

| Coluna             | Tipo Spark    | Tipo PostgreSQL | Descrição                             | Obrigatório |
|--------------------|---------------|-----------------|---------------------------------------|-------------|
| `id`               | IntegerType   | SERIAL          | PK                                    | Sim         |
| `nome`             | StringType    | TEXT            | Ex: `"temperature"`, `"pressure"`    | Sim         |
| `unidade`          | StringType    | TEXT            | Ex: `"celsius"`, `"bar"`             | Sim         |
| `valor_minimo`     | DoubleType    | NUMERIC         | Limite físico mínimo do sensor        | Sim         |
| `valor_maximo`     | DoubleType    | NUMERIC         | Limite físico máximo do sensor        | Sim         |
| `faixa_normal_min` | DoubleType    | NUMERIC         | Início da faixa operacional normal    | Sim         |
| `faixa_normal_max` | DoubleType    | NUMERIC         | Fim da faixa operacional normal       | Sim         |
| `_execution_date`  | DateType      | -               | Data de execução do job               | Sim         |
| `_processed_at`    | TimestampType | -               | Timestamp de processamento            | Sim         |
| `_data_source`     | StringType    | -               | Valor fixo: `"postgres"`             | Sim         |

---

### 1.6 bronze/postgres/leituras

**Origem:** `erp_legado.leituras`  
**Carga:** Incremental por data (`timestamp_leitura::DATE = execution_date`)  
**ETL:** `src/processamento/bronze/postgres/leituras.py`  
**Particionamento:** `_execution_date`

| Coluna               | Tipo Spark    | Tipo PostgreSQL  | Descrição                                      | Obrigatório |
|----------------------|---------------|------------------|------------------------------------------------|-------------|
| `id`                 | StringType    | UUID             | PK da leitura (UUID v4)                        | Sim         |
| `sensor_id`          | StringType    | VARCHAR(20)      | FK → sensores.id                               | Sim         |
| `valor`              | DoubleType    | NUMERIC(10,2)    | Valor medido pelo sensor                       | Sim         |
| `status_qualidade_id`| IntegerType   | INTEGER          | FK → status_qualidade.id                       | Sim         |
| `timestamp_leitura`  | TimestampType | TIMESTAMPTZ      | Momento da leitura (UTC)                       | Sim         |
| `is_anomalia`        | BooleanType   | BOOLEAN          | Indica leitura fora do range esperado          | Não         |
| `_execution_date`    | DateType      | -                | Partição da data de execução                   | Sim         |
| `_processed_at`      | TimestampType | -                | Timestamp de processamento                     | Sim         |
| `_data_source`       | StringType    | -                | Valor fixo: `"postgres"`                      | Sim         |

---

### 1.7 bronze/postgres/metadados_leitura

**Origem:** `erp_legado.metadados_leitura`  
**Carga:** Incremental por data (join com leituras)  
**ETL:** `src/processamento/bronze/postgres/metadados_leitura.py`  
**Particionamento:** `_execution_date`

| Coluna            | Tipo Spark    | Tipo PostgreSQL | Descrição                                   | Obrigatório |
|-------------------|---------------|-----------------|---------------------------------------------|-------------|
| `leitura_id`      | StringType    | UUID            | FK → leituras.id (PK desta tabela)          | Sim         |
| `versao_firmware` | StringType    | TEXT            | Versão do firmware do sensor no momento     | Não         |
| `nivel_bateria`   | IntegerType   | SMALLINT        | Nível de bateria em % (0–100)               | Não         |
| `forca_sinal_dbm` | IntegerType   | SMALLINT        | Força do sinal em dBm (negativo)            | Não         |
| `_execution_date` | DateType      | -               | Partição da data de execução                | Sim         |
| `_processed_at`   | TimestampType | -               | Timestamp de processamento                  | Sim         |
| `_data_source`    | StringType    | -               | Valor fixo: `"postgres"`                   | Sim         |

---

### 1.8 bronze/postgres/status_qualidade

**Origem:** `erp_legado.status_qualidade`  
**Carga:** Full load diário  
**ETL:** `src/processamento/bronze/postgres/status_qualidade.py`

| Coluna           | Tipo Spark    | Tipo PostgreSQL | Descrição                          | Obrigatório |
|------------------|---------------|-----------------|------------------------------------|-------------|
| `id`             | IntegerType   | SERIAL          | PK                                 | Sim         |
| `nome`           | StringType    | TEXT            | Ex: `"bom"`, `"suspeito"`, `"ruim"`| Sim         |
| `_execution_date`| DateType      | -               | Data de execução do job            | Sim         |
| `_processed_at`  | TimestampType | -               | Timestamp de processamento         | Sim         |
| `_data_source`   | StringType    | -               | Valor fixo: `"postgres"`          | Sim         |

---

### 1.9 bronze/mongo/equipamentos

**Origem:** MongoDB `Database.equipments`  
**Carga:** Full load (snapshot do catálogo)  
**ETL:** `src/processamento/bronze/mongo/equipamentos.py`

| Coluna                    | Tipo Spark         | Tipo BSON     | Descrição                                            | Obrigatório |
|---------------------------|--------------------|---------------|------------------------------------------------------|-------------|
| `equipamento_id`          | StringType         | String (`_id`)| Identificador único (ex: EQ-1234)                   | Sim         |
| `nome`                    | StringType         | String        | Nome do equipamento                                  | Sim         |
| `tipo`                    | StringType         | String        | Categoria (ex: `"compressor"`)                      | Sim         |
| `fabrica_id`              | StringType         | String        | factory.id desnormalizado                            | Sim         |
| `fabrica_nome`            | StringType         | String        | factory.name desnormalizado                          | Sim         |
| `latitude`                | DoubleType         | Double        | factory.location.lat desnormalizado                  | Não         |
| `longitude`               | DoubleType         | Double        | factory.location.lng desnormalizado                  | Não         |
| `sensors`                 | ArrayType(Struct)  | Array         | Lista de sensores `[{id, type, range:{min,max}}]`   | Sim         |
| `status`                  | StringType         | String        | `active`, `inactive`, `maintenance`                  | Sim         |
| `periodicidade_manutencao`| StringType         | String        | Ex: `"monthly"`, `"weekly"`                          | Não         |
| `_execution_date`         | DateType           | -             | Data de execução do job                              | Sim         |
| `_processed_at`           | TimestampType      | -             | Timestamp de processamento                           | Sim         |
| `_data_source`            | StringType         | -             | Valor fixo: `"mongo"`                               | Sim         |

---

### 1.10 bronze/factory_id=\*/measurement_type=\*/dt=\* (Kafka)

**Origem:** Apache Kafka - tópico de sensores IoT  
**Carga:** Streaming contínuo via `bronze_consumer.py`  
**Formato:** NDJSON (`.ndjson`), um arquivo por batch  
**Particionamento Hive:** `factory_id` / `measurement_type` / `dt=YYYY-MM-DD`

| Campo                | Tipo JSON  | Descrição                                                   | Obrigatório |
|----------------------|------------|-------------------------------------------------------------|-------------|
| `event_id`           | String     | UUID único do evento                                        | Sim         |
| `sensor_id`          | String     | Identificador do sensor (ex: SENS-001-TEMP)                 | Sim         |
| `equipment_id`       | String     | Identificador do equipamento (ex: EQ-1234)                  | Sim         |
| `factory_id`         | String     | Identificador da fábrica - também chave de partição Hive    | Sim         |
| `measurement_type`   | String     | `temperature`, `humidity`, `pressure`, `vibration`, `current`| Sim        |
| `value`              | Number     | Valor medido                                                | Sim         |
| `unit`               | String     | Unidade de medida (ex: `"celsius"`)                        | Sim         |
| `timestamp`          | String     | ISO 8601 UTC (ex: `"2025-03-15T14:30:00.123Z"`)            | Sim         |
| `quality`            | String     | `good`, `warning`, `bad`                                    | Sim         |
| `is_anomaly`         | Boolean    | Indica leitura fora do range do sensor                      | Não         |
| `metadata`           | Object     | `{firmware_version: String, battery_level: Integer}`        | Não         |
| `_kafka_topic`       | String     | Tópico Kafka de origem (metadado de transporte)             | Sim         |
| `_kafka_partition`   | Integer    | Partição Kafka                                              | Sim         |
| `_kafka_offset`      | Long       | Offset dentro da partição                                   | Sim         |
| `_kafka_key`         | String     | Chave de mensagem Kafka                                     | Não         |
| `_ingested_at`       | String     | ISO 8601 - timestamp de gravação no MinIO pelo consumer     | Sim         |

---

## 2. Camada Silver

**Formato:** Delta Lake (Parquet colunar + `_delta_log/` transacional)  
**Modo de escrita:** `upsert` via Delta Merge (`previous_delete=False`)  
**Localização:** `s3a://silver/{source}/{table}/`  
**Base class:** `src/processamento/silver/base_silver_etl.py`

> Todos os campos `_execution_date`, `_processed_at` e `_data_source` são herdados da camada Bronze e re-escritos pelo `add_metadata_columns()` do `AbstractETL` no momento do processamento Silver.

### 2.1 silver/postgres/fabricas

**Origem:** `bronze/postgres/fabricas`  
**Carga:** Full load (upsert por `id`)  
**ETL:** `src/processamento/silver/postgres/fabricas.py`  
**Chave de merge:** `target.id = source.id`

| Coluna           | Tipo Spark    | Transformação Silver              | Obrigatório |
|------------------|---------------|-----------------------------------|-------------|
| `id`             | StringType    | Cast VARCHAR(20) - chave natural  | Sim         |
| `nome`           | StringType    | Cast StringType                   | Sim         |
| `latitude`       | DoubleType    | Cast DoubleType                   | Sim         |
| `longitude`      | DoubleType    | Cast DoubleType                   | Sim         |
| `_execution_date`| DateType      | Herdado + re-escrito              | Sim         |
| `_processed_at`  | TimestampType | Atualizado no processamento       | Sim         |
| `_data_source`   | StringType    | Herdado                           | Sim         |

**Qualidade (PyDeequ - Error):** `id`, `nome`, `latitude`, `longitude` completos.

---

### 2.2 silver/postgres/equipamentos

**Origem:** `bronze/postgres/equipamentos`  
**Carga:** Full load (upsert por `id`)  
**ETL:** `src/processamento/silver/postgres/equipamentos.py`  
**Chave de merge:** `target.id = source.id`

| Coluna               | Tipo Spark    | Transformação Silver                                        | Obrigatório |
|----------------------|---------------|-------------------------------------------------------------|-------------|
| `id`                 | StringType    | Cast VARCHAR(20)                                            | Sim         |
| `nome`               | StringType    | Cast StringType                                             | Sim         |
| `tipo_equipamento_id`| IntegerType   | Cast IntegerType                                            | Sim         |
| `fabrica_id`         | StringType    | Cast VARCHAR(20)                                            | Sim         |
| `status`             | StringType    | Filter: `{ativo, inativo, manutencao}`                      | Sim         |
| `data_instalacao`    | DateType      | Cast DateType                                               | Não         |
| `_execution_date`    | DateType      | -                                                           | Sim         |
| `_processed_at`      | TimestampType | -                                                           | Sim         |
| `_data_source`       | StringType    | -                                                           | Sim         |

**Qualidade (PyDeequ - Error):** `id`, `nome`, `tipo_equipamento_id`, `fabrica_id`, `status` completos; `status ∈ {ativo, inativo, manutencao}`.

---

### 2.3 silver/postgres/tipos_equipamento

**Origem:** `bronze/postgres/tipos_equipamento`  
**Carga:** Full load (upsert por `id`)  
**ETL:** `src/processamento/silver/postgres/tipos_equipamento.py`

| Coluna           | Tipo Spark    | Transformação Silver | Obrigatório |
|------------------|---------------|----------------------|-------------|
| `id`             | IntegerType   | Cast IntegerType     | Sim         |
| `nome`           | StringType    | Cast StringType      | Sim         |
| `_execution_date`| DateType      | -                    | Sim         |
| `_processed_at`  | TimestampType | -                    | Sim         |
| `_data_source`   | StringType    | -                    | Sim         |

**Qualidade (PyDeequ - Error):** `id`, `nome` completos; `nome` único.

---

### 2.4 silver/postgres/sensores

**Origem:** `bronze/postgres/sensores`  
**Carga:** Full load (upsert por `id`)  
**ETL:** `src/processamento/silver/postgres/sensores.py`

| Coluna            | Tipo Spark    | Transformação Silver                             | Obrigatório |
|-------------------|---------------|--------------------------------------------------|-------------|
| `id`              | StringType    | Cast VARCHAR(20)                                 | Sim         |
| `equipamento_id`  | StringType    | Cast VARCHAR(20)                                 | Sim         |
| `tipo_medicao_id` | IntegerType   | Cast IntegerType                                 | Sim         |
| `status`          | StringType    | Filter: `{ativo, inativo, manutencao}`           | Sim         |
| `data_instalacao` | DateType      | Cast DateType                                    | Não         |
| `_execution_date` | DateType      | -                                                | Sim         |
| `_processed_at`   | TimestampType | -                                                | Sim         |
| `_data_source`    | StringType    | -                                                | Sim         |

**Qualidade (PyDeequ - Error):** `id`, `equipamento_id`, `tipo_medicao_id`, `status` completos; `status ∈ {ativo, inativo, manutencao}`.

---

### 2.5 silver/postgres/tipos_medicao

**Origem:** `bronze/postgres/tipos_medicao`  
**Carga:** Full load (upsert por `id`)  
**ETL:** `src/processamento/silver/postgres/tipos_medicao.py`

| Coluna             | Tipo Spark    | Transformação Silver                                    | Obrigatório |
|--------------------|---------------|---------------------------------------------------------|-------------|
| `id`               | IntegerType   | Cast IntegerType                                        | Sim         |
| `nome`             | StringType    | Cast StringType                                         | Sim         |
| `unidade`          | StringType    | Cast StringType                                         | Sim         |
| `valor_minimo`     | DoubleType    | Cast DoubleType; Filter: `valor_minimo < valor_maximo`  | Sim         |
| `valor_maximo`     | DoubleType    | Cast DoubleType                                         | Sim         |
| `faixa_normal_min` | DoubleType    | Cast DoubleType; Filter: `faixa_normal_min < faixa_normal_max` | Sim  |
| `faixa_normal_max` | DoubleType    | Cast DoubleType                                         | Sim         |
| `_execution_date`  | DateType      | -                                                       | Sim         |
| `_processed_at`    | TimestampType | -                                                       | Sim         |
| `_data_source`     | StringType    | -                                                       | Sim         |

---

### 2.6 silver/postgres/leituras

**Origem:** `bronze/postgres/leituras`  
**Carga:** Incremental - filtra `_execution_date == execution_date` na bronze  
**ETL:** `src/processamento/silver/postgres/leituras.py`  
**Particionamento:** `_execution_date`  
**Chave de merge:** `target.id = source.id`

| Coluna               | Tipo Spark    | Transformação Silver         | Obrigatório |
|----------------------|---------------|------------------------------|-------------|
| `id`                 | StringType    | Cast UUID → String           | Sim         |
| `sensor_id`          | StringType    | Cast VARCHAR(20)             | Sim         |
| `valor`              | DoubleType    | Cast DoubleType              | Sim         |
| `status_qualidade_id`| IntegerType   | Cast IntegerType             | Sim         |
| `timestamp_leitura`  | TimestampType | Cast TimestampType           | Sim         |
| `is_anomalia`        | BooleanType   | Cast BooleanType             | Não         |
| `_execution_date`    | DateType      | Partição de reprocessamento  | Sim         |
| `_processed_at`      | TimestampType | -                            | Sim         |
| `_data_source`       | StringType    | -                            | Sim         |

**Qualidade (PyDeequ - Error):** `id`, `sensor_id`, `valor`, `status_qualidade_id`, `timestamp_leitura` completos.

---

### 2.7 silver/postgres/metadados_leitura

**Origem:** `bronze/postgres/metadados_leitura`  
**Carga:** Incremental  
**ETL:** `src/processamento/silver/postgres/metadados_leitura.py`  
**Particionamento:** `_execution_date`  
**Chave de merge:** `target.leitura_id = source.leitura_id`

| Coluna            | Tipo Spark    | Transformação Silver | Obrigatório |
|-------------------|---------------|----------------------|-------------|
| `leitura_id`      | StringType    | Cast UUID → String   | Sim         |
| `versao_firmware` | StringType    | Cast StringType      | Não         |
| `nivel_bateria`   | IntegerType   | Cast IntegerType     | Não         |
| `forca_sinal_dbm` | IntegerType   | Cast IntegerType     | Não         |
| `_execution_date` | DateType      | -                    | Sim         |
| `_processed_at`   | TimestampType | -                    | Sim         |
| `_data_source`    | StringType    | -                    | Sim         |

---

### 2.8 silver/postgres/status_qualidade

**Origem:** `bronze/postgres/status_qualidade`  
**Carga:** Full load (upsert por `id`)  
**ETL:** `src/processamento/silver/postgres/status_qualidade.py`

| Coluna           | Tipo Spark    | Transformação Silver | Obrigatório |
|------------------|---------------|----------------------|-------------|
| `id`             | IntegerType   | Cast IntegerType     | Sim         |
| `nome`           | StringType    | Cast StringType      | Sim         |
| `_execution_date`| DateType      | -                    | Sim         |
| `_processed_at`  | TimestampType | -                    | Sim         |
| `_data_source`   | StringType    | -                    | Sim         |

---

### 2.9 silver/mongo/equipamentos

**Origem:** `bronze/mongo/equipamentos`  
**Carga:** Full load (upsert por `equipamento_id`)  
**ETL:** `src/processamento/silver/mongo/equipamentos.py`  
**Chave de merge:** `target.equipamento_id = source.equipamento_id`

> O array `sensors` é preservado como estrutura aninhada para uso na Gold layer.

| Coluna                    | Tipo Spark         | Transformação Silver                      | Obrigatório |
|---------------------------|--------------------|-------------------------------------------|-------------|
| `equipamento_id`          | StringType         | Cast StringType (MongoDB `_id`)           | Sim         |
| `nome`                    | StringType         | Cast StringType                           | Sim         |
| `tipo`                    | StringType         | Cast StringType                           | Sim         |
| `fabrica_id`              | StringType         | Cast StringType                           | Sim         |
| `fabrica_nome`            | StringType         | Cast StringType                           | Sim         |
| `latitude`                | DoubleType         | Cast DoubleType                           | Não         |
| `longitude`               | DoubleType         | Cast DoubleType                           | Não         |
| `sensors`                 | ArrayType(Struct)  | Mantido como struct aninhado              | Sim         |
| `status`                  | StringType         | Filter: `{active, inactive, maintenance}` | Sim         |
| `periodicidade_manutencao`| StringType         | Cast StringType                           | Não         |
| `_execution_date`         | DateType           | -                                         | Sim         |
| `_processed_at`           | TimestampType      | -                                         | Sim         |
| `_data_source`            | StringType         | -                                         | Sim         |

**Qualidade (PyDeequ - Error):** `equipamento_id`, `nome`, `tipo`, `fabrica_id`, `status` completos; `status ∈ {active, inactive, maintenance}`.

---

### 2.10 silver/kafka/sensor_events

**Origem:** `bronze/factory_id=*/measurement_type=*/dt={execution_date}/*`  
**Carga:** Incremental por `dt=execution_date`  
**ETL:** `src/processamento/silver/kafka/sensor_events.py`  
**Particionamento:** `measurement_type`, `_execution_date`  
**Chave de merge:** `target.event_id = source.event_id`

| Coluna             | Tipo Spark    | Transformação Silver                                    | Obrigatório |
|--------------------|---------------|---------------------------------------------------------|-------------|
| `event_id`         | StringType    | Cast StringType                                         | Sim         |
| `sensor_id`        | StringType    | Cast StringType                                         | Sim         |
| `equipment_id`     | StringType    | Cast StringType                                         | Sim         |
| `factory_id`       | StringType    | Cast StringType (partição Hive lida como coluna)        | Sim         |
| `measurement_type` | StringType    | Cast StringType; Filter: `{temperature, humidity, pressure, vibration, current}`| Sim |
| `value`            | DoubleType    | Cast DoubleType                                         | Sim         |
| `unit`             | StringType    | Cast StringType                                         | Não         |
| `timestamp`        | TimestampType | Cast TimestampType                                      | Sim         |
| `quality`          | StringType    | Cast StringType; Filter: `{good, warning, bad}`        | Sim         |
| `is_anomaly`       | BooleanType   | Cast BooleanType                                        | Não         |
| `versao_firmware`  | StringType    | `metadata.firmware_version` achatado                   | Não         |
| `nivel_bateria`    | IntegerType   | `metadata.battery_level` achatado                      | Não         |
| `_ingested_at`     | TimestampType | Preservado da bronze; cast TimestampType               | Sim         |
| `_execution_date`  | DateType      | Partição de reprocessamento                             | Sim         |
| `_processed_at`    | TimestampType | -                                                       | Sim         |
| `_data_source`     | StringType    | -                                                       | Sim         |

> **Campos removidos na Silver:** `metadata` (achatado em `versao_firmware` e `nivel_bateria`), `_kafka_offset`, `_kafka_partition`, `_kafka_topic`, `_kafka_key`.

**Qualidade (PyDeequ - Error):** `event_id`, `sensor_id`, `equipment_id`, `factory_id`, `measurement_type`, `value`, `timestamp`, `quality` completos; `quality ∈ {good, warning, bad}`; `measurement_type ∈ {temperature, humidity, pressure, vibration, current}`.

---

## 3. Camada Gold - Modelagem Dimensional Proposta

**Formato:** Delta Lake  
**Localização:** `s3a://gold/{table}/`  
**Granularidade:** Hora (facts) e Dia (anomalias)  
**Paradigma:** Star Schema (Kimball)

### 3.1 Star Schema

```
                         ┌─────────────────┐
                         │   dim_tempo      │
                         │─────────────────│
                         │ PK date_key      │
                         │    data          │
                         │    hora          │
                         │    dia_semana    │
                         │    mes / ano     │
                         └────────┬────────┘
                                  │
        ┌────────────┐            │            ┌──────────────────┐
        │ dim_fabrica│            │            │  dim_tipo_medicao│
        │────────────│            │            │──────────────────│
        │ PK fabrica_id           │            │ PK tipo_medicao_id
        │    nome    │            │            │    nome / unidade│
        │    lat/lng │            │            │    faixa_normal  │
        └─────┬──────┘            │            └────────┬─────────┘
              │                   │                     │
              │      ┌────────────▼─────────────┐       │
              │      │    fct_leituras_hora       │       │
              └──────│───────────────────────────│───────┘
                     │ FK date_key               │
                     │ FK sensor_id              │
                     │ FK equipment_id           │
                     │ FK fabrica_id             │
                     │ FK tipo_medicao_id        │
                     │ measurement_type          │
                     │ avg_valor                 │
                     │ min_valor / max_valor     │
                     │ count_leituras            │
                     │ count_anomalias           │
                     │ pct_qualidade_boa         │
                     └───────────┬───────────────┘
                                 │
              ┌──────────────────┘
              │
        ┌─────▼──────────┐       ┌─────────────────┐
        │ dim_equipamento│       │   dim_sensor     │
        │────────────────│       │─────────────────│
        │ PK equipment_id│       │ PK sensor_id     │
        │    nome / tipo │       │    equipment_id  │
        │    fabrica_id  │       │    tipo_medicao_id
        │    status      │       │    status        │
        │    periodicidade       └─────────────────┘
        └────────────────┘
```

---

### 3.2 dim_tempo

**Granularidade:** Hora  
**Carga:** Gerada programaticamente (sem ETL de fonte)

| Coluna       | Tipo Spark  | Descrição                                      |
|--------------|-------------|------------------------------------------------|
| `date_key`   | LongType    | PK - formato `YYYYMMDDHH` (ex: `2026061914`)   |
| `data`       | DateType    | Data completa                                  |
| `hora`       | IntegerType | Hora do dia (0–23)                             |
| `dia_semana` | IntegerType | 1=Dom, 2=Seg, …, 7=Sáb (padrão ISO-like)       |
| `semana_ano` | IntegerType | Semana ISO do ano (1–53)                       |
| `mes`        | IntegerType | Mês (1–12)                                     |
| `trimestre`  | IntegerType | Trimestre (1–4)                                |
| `ano`        | IntegerType | Ano (ex: 2026)                                 |
| `nome_mes`   | StringType  | Nome por extenso (ex: `"Junho"`)               |

---

### 3.3 dim_fabrica

**Fonte:** `silver/postgres/fabricas`  
**SCD Tipo 1** (sobrescreve quando nome/coordenadas mudam)

| Coluna      | Tipo Spark  | Descrição                        |
|-------------|-------------|----------------------------------|
| `fabrica_id`| StringType  | PK - ex: `"FAB-SP-01"`          |
| `nome`      | StringType  | Nome da fábrica                  |
| `latitude`  | DoubleType  | Latitude geográfica              |
| `longitude` | DoubleType  | Longitude geográfica             |

---

### 3.4 dim_equipamento

**Fonte:** `silver/postgres/equipamentos` + `silver/mongo/equipamentos`  
**Enriquecimento:** `periodicidade_manutencao` vem do MongoDB  
**SCD Tipo 1**

| Coluna                    | Tipo Spark  | Descrição                                          |
|---------------------------|-------------|----------------------------------------------------|
| `equipment_id`            | StringType  | PK - ex: `"EQ-1234"`                              |
| `nome`                    | StringType  | Nome do equipamento                                |
| `tipo_equipamento`        | StringType  | Nome do tipo (join com tipos_equipamento)          |
| `fabrica_id`              | StringType  | FK → dim_fabrica                                   |
| `status`                  | StringType  | `ativo`, `inativo`, `manutencao`                   |
| `data_instalacao`         | DateType    | Data de instalação                                 |
| `periodicidade_manutencao`| StringType  | Do MongoDB: `monthly`, `weekly`, `daily`           |

---

### 3.5 dim_sensor

**Fonte:** `silver/postgres/sensores`  
**SCD Tipo 1**

| Coluna           | Tipo Spark  | Descrição                      |
|------------------|-------------|--------------------------------|
| `sensor_id`      | StringType  | PK - ex: `"SENS-001-TEMP"`    |
| `equipment_id`   | StringType  | FK → dim_equipamento           |
| `tipo_medicao_id`| IntegerType | FK → dim_tipo_medicao          |
| `status`         | StringType  | `ativo`, `inativo`, `manutencao`|
| `data_instalacao`| DateType    | Data de instalação             |

---

### 3.6 dim_tipo_medicao

**Fonte:** `silver/postgres/tipos_medicao`  
**SCD Tipo 1**

| Coluna             | Tipo Spark  | Descrição                           |
|--------------------|-------------|-------------------------------------|
| `tipo_medicao_id`  | IntegerType | PK                                  |
| `nome`             | StringType  | Ex: `"temperature"`, `"pressure"`  |
| `unidade`          | StringType  | Ex: `"celsius"`, `"bar"`           |
| `valor_minimo`     | DoubleType  | Limite físico mínimo do sensor      |
| `valor_maximo`     | DoubleType  | Limite físico máximo do sensor      |
| `faixa_normal_min` | DoubleType  | Início da operação normal           |
| `faixa_normal_max` | DoubleType  | Fim da operação normal              |

---

### 3.7 fct_leituras_hora

**Fonte:** `silver/kafka/sensor_events` + `silver/postgres/leituras`  
**Granularidade:** Por sensor, por hora  
**Carga:** Incremental (janela da hora anterior)  
**Particionamento:** `measurement_type`, `_execution_date`  
**Responde a:** Req. 1 (Dashboard Operacional), Req. 3 (Relatórios Diários), Req. 5 (Análise Preditiva)

| Coluna              | Tipo Spark    | Descrição                                              |
|---------------------|---------------|--------------------------------------------------------|
| `date_key`          | LongType      | FK → dim_tempo (YYYYMMDDHH)                            |
| `sensor_id`         | StringType    | FK → dim_sensor                                        |
| `equipment_id`      | StringType    | FK → dim_equipamento (desnormalizado para performance) |
| `fabrica_id`        | StringType    | FK → dim_fabrica (desnormalizado)                      |
| `tipo_medicao_id`   | IntegerType   | FK → dim_tipo_medicao                                  |
| `measurement_type`  | StringType    | Redundante com dim_tipo_medicao - filtro de partição   |
| `avg_valor`         | DoubleType    | Média das leituras na hora                             |
| `min_valor`         | DoubleType    | Mínimo das leituras na hora                            |
| `max_valor`         | DoubleType    | Máximo das leituras na hora                            |
| `stddev_valor`      | DoubleType    | Desvio padrão - útil para ML preditivo                 |
| `count_leituras`    | LongType      | Número de leituras no intervalo                        |
| `count_anomalias`   | LongType      | Leituras com `is_anomaly = true`                       |
| `pct_qualidade_boa` | DoubleType    | % de leituras com `quality = "good"` (0.0–1.0)        |
| `_execution_date`   | DateType      | Partição de reprocessamento                            |
| `_processed_at`     | TimestampType | Timestamp de geração da agregação                      |

---

### 3.8 fct_anomalias_dia

**Fonte:** `silver/kafka/sensor_events` + `silver/postgres/leituras`  
**Granularidade:** Por sensor, por dia  
**Carga:** Incremental diário  
**Particionamento:** `_execution_date`  
**Responde a:** Req. 2 (Alertas de Anomalia), Req. 3 (Relatórios Diários), Req. 4 (Histórico Completo)

| Coluna             | Tipo Spark    | Descrição                                              |
|--------------------|---------------|--------------------------------------------------------|
| `data`             | DateType      | FK → dim_tempo (granularidade dia)                     |
| `sensor_id`        | StringType    | FK → dim_sensor                                        |
| `equipment_id`     | StringType    | FK → dim_equipamento                                   |
| `fabrica_id`       | StringType    | FK → dim_fabrica                                       |
| `measurement_type` | StringType    | Tipo de medição                                        |
| `total_leituras`   | LongType      | Total de leituras no dia para este sensor              |
| `total_anomalias`  | LongType      | Total de anomalias detectadas no dia                   |
| `taxa_anomalia`    | DoubleType    | `total_anomalias / total_leituras` (0.0–1.0)           |
| `max_valor_dia`    | DoubleType    | Valor máximo registrado no dia                         |
| `min_valor_dia`    | DoubleType    | Valor mínimo registrado no dia                         |
| `_execution_date`  | DateType      | Partição de reprocessamento                            |
| `_processed_at`    | TimestampType | Timestamp de geração                                   |

---

## 4. Linhagem de Dados

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

## 5. Convenções e Metadados Técnicos

### Campos de metadados presentes em todas as tabelas

| Campo            | Tipo          | Adicionado por              | Descrição                                              |
|------------------|---------------|-----------------------------|--------------------------------------------------------|
| `_execution_date`| DateType      | `AbstractETL.add_metadata_columns()` | Data de execução do job (parâmetro `--execution_date`) |
| `_processed_at`  | TimestampType | `AbstractETL.add_metadata_columns()` | Timestamp Spark no momento de `load()`                 |
| `_data_source`   | StringType    | `AbstractETL.add_metadata_columns()` | Nome do source_name derivado do `table_path`           |

### Ambientes

| Ambiente | Bucket Bronze   | Bucket Silver   | Uso                      |
|----------|-----------------|-----------------|--------------------------|
| `prd`    | `bronze`        | `silver`        | Produção - ambiente ativo|
| `hmg`    | `bronze-hmg`    | `silver-hmg`    | Homologação - inativo    |

### Tipos de carga

| Tipo              | Descrição                                                                 |
|-------------------|---------------------------------------------------------------------------|
| Full load         | Lê toda a tabela de origem; upsert com `previous_delete=False` na Silver  |
| Incremental       | Filtra pela `_execution_date` ou `timestamp` da execução; mesmo upsert    |

### Formato dos IDs

| Entidade          | Tipo     | Exemplo                                  |
|-------------------|----------|------------------------------------------|
| Fábrica           | VARCHAR(20) | `FAB-SP-01`, `FAB-RJ-02`             |
| Equipamento       | VARCHAR(20) | `EQ-1234`, `EQ-5678`                 |
| Sensor            | VARCHAR(20) | `SENS-001-TEMP`, `SENS-002-VIBR`     |
| Leitura (PG)      | UUID     | `550e8400-e29b-41d4-a716-446655440000`    |
| Evento Kafka      | UUID     | `550e8400-e29b-41d4-a716-446655440001`    |
| Equipamento (Mongo)| String  | `EQ-1234` (mesmo identificador do PG)    |
