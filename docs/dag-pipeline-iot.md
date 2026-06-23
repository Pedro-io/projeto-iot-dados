# DAG: pipeline_iot

Orquestra o pipeline batch diário completo da plataforma IoT, percorrendo as três camadas da arquitetura Medallion: **Bronze → Silver → Gold**.

---

## Visão geral

| Atributo | Valor |
|---|---|
| DAG ID | `pipeline_iot` |
| Arquivo | `src/dags/pipeline_iot.py` |
| Schedule | `@daily` (meia-noite UTC) |
| Timezone | UTC |
| Start date | 2026-01-01 |
| Catchup | Desativado |
| Max active runs | 1 (sem execuções concorrentes) |
| Retries por task | 1 (intervalo de 5 min) |

---

## Fluxo de execução

```
bronze (paralelo)
  ├── postgres_fabricas
  ├── postgres_equipamentos
  ├── postgres_sensores
  ├── postgres_tipos_equipamento
  ├── postgres_tipos_medicao
  ├── postgres_status_qualidade
  ├── postgres_leituras
  ├── postgres_metadados_leitura
  └── mongo_equipamentos
        │
        ▼
silver (paralelo)
  ├── postgres_fabricas
  ├── postgres_equipamentos
  ├── postgres_sensores
  ├── postgres_tipos_equipamento
  ├── postgres_tipos_medicao
  ├── postgres_status_qualidade
  ├── postgres_leituras
  ├── postgres_metadados_leitura
  ├── mongo_equipamentos
  └── kafka_sensor_events
        │
        ▼
gold
  ├── dimensions (paralelo)
  │     ├── dim_fabrica
  │     ├── dim_equipamento
  │     ├── dim_sensor
  │     ├── dim_tipo_medicao
  │     └── dim_tempo
  │           │
  └── facts (após dimensions)
        ├── fct_leituras_hora
        └── fct_anomalias_dia
```

A camada inteira anterior precisa ser concluída com sucesso antes da próxima iniciar. Dentro de cada camada, todas as tasks rodam em paralelo (exceto Gold, onde os fatos aguardam as dimensões).

---

## Como cada task executa

Cada task usa um `BashOperator` que chama `docker exec` no container `spark-master`, invocando `spark-submit` com o script correspondente:

```bash
docker exec \
  -e MINIO_ROOT_USER=$MINIO_ROOT_USER \
  -e MINIO_ROOT_PASSWORD=$MINIO_ROOT_PASSWORD \
  -e PYTHONPATH=/jobs \
  spark-master \
  /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    /jobs/processamento/<camada>/<fonte>/<tabela>.py \
    --execution_date {{ ds }} \
    --environment prd \
    --owner airflow
```

O `{{ ds }}` é substituído pelo Airflow com a data lógica da execução no formato `YYYY-MM-DD`.

O worker do Airflow acessa o Docker do host via socket (`/var/run/docker.sock`), portanto o `spark-master` precisa estar rodando antes de iniciar a DAG.

---

## Argumentos por grupo

### Bronze — PostgreSQL

Além dos args comuns, as tasks de PostgreSQL recebem credenciais JDBC:

| Argumento | Valor |
|---|---|
| `--jdbc_url` | `jdbc:postgresql://postgres:5432/$POSTGRES_DB` |
| `--jdbc_user` | `$POSTGRES_USER` |
| `--jdbc_password` | `$POSTGRES_PASSWORD` |

### Bronze — MongoDB

As tasks de MongoDB recebem a URI e os parâmetros da coleção:

| Argumento | Valor |
|---|---|
| `--mongo_uri` | `mongodb://$MONGO_USER:$MONGO_PASS@mongodb:27017` |
| `--mongo_database` | `Database` |
| `--mongo_collection` | `equipments` |

### Silver e Gold

Recebem apenas os args comuns: `--execution_date`, `--environment`, `--owner`.

---

## Variáveis de ambiente necessárias

As variáveis abaixo precisam estar no `.env` da raiz do projeto. O compose do Airflow as injeta automaticamente nos containers via `env_file: ../../.env`.

| Variável | Usada em |
|---|---|
| `MINIO_ROOT_USER` | Todas as tasks (acesso ao MinIO/S3A) |
| `MINIO_ROOT_PASSWORD` | Todas as tasks |
| `POSTGRES_DB` | Bronze PostgreSQL |
| `POSTGRES_USER` | Bronze PostgreSQL |
| `POSTGRES_PASSWORD` | Bronze PostgreSQL |
| `MONGO_USER` | Bronze MongoDB |
| `MONGO_PASS` | Bronze MongoDB |

---

## Como acionar manualmente

Via interface web do Airflow (`http://localhost:8080`):

1. Abra a DAG `pipeline_iot`
2. Clique em **Trigger DAG** (ícone de play)
3. Informe a `logical_date` desejada se quiser reprocessar uma data específica

Via CLI (dentro do container):

```bash
# Acionar com a data de hoje
docker exec -it airflow-airflow-scheduler-1 \
  airflow dags trigger pipeline_iot

# Acionar para uma data específica
docker exec -it airflow-airflow-scheduler-1 \
  airflow dags trigger pipeline_iot --logical-date 2026-06-20T00:00:00+00:00
```

---

## Como monitorar

| O que verificar | Onde |
|---|---|
| Status das tasks (success/failed/running) | Airflow UI → DAG → Grid View |
| Logs de cada task | Airflow UI → task → Log |
| Logs do Spark job | `docker logs spark-master` ou Spark UI (`http://localhost:8085`) |
| Dados gravados no MinIO | MinIO Console (`http://localhost:9001`) → buckets `bronze`, `silver`, `gold` |

---

## Troubleshooting

**Task falha com `docker: command not found`**
O binário `/usr/bin/docker` não está disponível no worker. Verifique se o volume `/usr/bin/docker:/usr/bin/docker:ro` está montado no serviço `airflow-worker` em `infra/airflow/docker-compose.yaml`.

**Task falha com `Cannot connect to the Docker daemon`**
O socket `/var/run/docker.sock` não está acessível. Verifique se o volume `/var/run/docker.sock:/var/run/docker.sock` está montado no worker.

**Task falha com `container spark-master is not running`**
A stack de storage e Spark precisa estar no ar antes de executar a DAG. Suba com:
```bash
docker compose --env-file .env -f infra/docker/docker-compose.yml up -d
```

**`No transformed data!` nos logs do Spark**
O ETL não encontrou dados para processar na `execution_date`. Verifique se há dados no bucket `bronze` para a partição correspondente.

---

## Adicionar um novo ETL à DAG

1. Crie o script em `src/processamento/<camada>/<fonte>/<tabela>.py` seguindo o padrão `AbstractETL`
2. Adicione a entrada na lista correspondente em `pipeline_iot.py`:
   - Bronze PostgreSQL → lista `pg_tables`
   - Bronze MongoDB → nova task com `_MONGO`
   - Silver → lista `silver_scripts`
   - Gold dimensão → lista do `TaskGroup("dimensions")`
   - Gold fato → novo `BashOperator` dentro do `TaskGroup("facts")`
3. O Airflow detecta a mudança automaticamente (o diretório `src/dags/` é montado em tempo real).
