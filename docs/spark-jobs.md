## Spark - Como rodar e testar jobs

Este documento descreve como executar scripts PySpark no cluster local, acompanhar logs e estruturar jobs para o processamento das camadas Silver e Gold.

A stack Spark é definida em `infra/docker/docker-compose.yml` e composta por dois containers: `spark-master` e `spark-worker`.

Para detalhes da imagem Spark e do Dockerfile usado pelo cluster, veja `docs/spark-dockerfile.md`.

---

## Pré-requisitos

A stack de storage deve estar no ar antes de rodar qualquer job:

```bash
docker compose --env-file .env -f infra/docker/docker-compose.yml up -d
```

Verifique se o worker está registrado acessando a UI do master:

* Spark Master UI: http://localhost:8085

O worker aparece na seção "Workers" com status `ALIVE` alguns segundos após subir.

### Criar os buckets no MinIO (apenas na primeira vez)

Os jobs Bronze gravam em `s3a://bronze-hmg/`. O bucket precisa existir antes do primeiro job - o Spark não cria automaticamente:

```bash
docker exec minio mc alias set local http://localhost:9000 $MINIO_ROOT_USER $MINIO_ROOT_PASSWORD --quiet
docker exec minio mc mb local/bronze-hmg --quiet
```

Repita para outros ambientes (`bronze-prd`, `silver-hmg`, etc.) conforme necessário.

---

## Submeter um job

O comando base para rodar qualquer script é:

```bash
docker exec \
  -e MINIO_ROOT_USER=<usuario> \
  -e MINIO_ROOT_PASSWORD=<senha> \
  -e PYTHONPATH=/jobs \
  spark-master \
  /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    /jobs/<caminho-do-script>.py
```

As variáveis `-e` são obrigatórias:

| Variável | Motivo |
|---|---|
| `MINIO_ROOT_USER` / `MINIO_ROOT_PASSWORD` | Credenciais S3A lidas via `os.getenv()` nos jobs |
| `PYTHONPATH=/jobs` | Permite imports entre módulos dentro de `src/` |

O diretório `src/` do projeto é montado em `/jobs` dentro dos containers. Então o mapeamento é:

```
src/processamento/meu_script.py  ->  /jobs/processamento/meu_script.py
src/silver/processar_sensores.py ->  /jobs/silver/processar_sensores.py
```

---

## Validar a conexão com o MinIO

Existe um job de validação em `src/processamento/test_spark_conexao.py` que grava e lê um Parquet no MinIO para confirmar que o S3A está funcionando.

```bash
docker exec spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /jobs/processamento/test_spark_conexao.py
```

Saída esperada:

```
[OK] Escrita em s3a://bronze/test/validacao_spark
+-----------+-----------+
| sensor_id |temperatura|
+-----------+-----------+
| sensor-001|       22.5|
| sensor-002|       18.3|
+-----------+-----------+
[OK] Leitura ok - 2 linhas
```

---

## Estrutura mínima de um job PySpark

Todo job que acessa o MinIO precisa configurar as credenciais S3A na `SparkSession`:

```python
import os
from pyspark.sql import SparkSession

MINIO_USER = os.environ["MINIO_ROOT_USER"]
MINIO_PASS = os.environ["MINIO_ROOT_PASSWORD"]

spark = (
    SparkSession.builder
    .appName("nome-do-job")
    .config("spark.hadoop.fs.s3a.access.key", MINIO_USER)
    .config("spark.hadoop.fs.s3a.secret.key", MINIO_PASS)
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# lógica do job aqui

spark.stop()
```

As demais configs S3A (endpoint, path style, SSL) já estão em `infra/docker/spark/spark-defaults.conf` e são aplicadas automaticamente.

---

## Acompanhar logs de um job

Os logs aparecem diretamente no terminal ao rodar o `spark-submit`. Para reduzir o ruído de logs INFO do Spark, o job deve chamar:

```python
spark.sparkContext.setLogLevel("WARN")
```

Para acompanhar logs do container master em tempo real (útil quando o job trava):

```bash
docker logs -f spark-master
```

Para logs do worker (onde a execução de fato acontece):

```bash
docker logs -f spark-worker
```

---

## Testar um script localmente (sem o cluster)

Durante o desenvolvimento de um job, é possível rodá-lo em modo local diretamente dentro do container, sem usar o worker. Útil para iteração rápida:

```bash
docker exec spark-master /opt/spark/bin/spark-submit \
  --master local[*] \
  /jobs/processamento/meu_script.py
```

`local[*]` significa: use todos os cores disponíveis no container, sem enviar tarefas para o worker. O job roda mais rápido para volumes pequenos de dados e facilita o debug.

Quando o job estiver validado, troque para `spark://spark-master:7077` para usar o cluster completo.

---

## Passar argumentos para um job

```python
# no script
import sys
caminho_entrada = sys.argv[1]
caminho_saida   = sys.argv[2]
```

```bash
docker exec spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /jobs/silver/processar_sensores.py \
  s3a://bronze/factory_id=F1/ \
  s3a://silver/sensores/
```

---

## Rebuild da imagem Spark

Necessário quando o `Dockerfile` ou o `spark-defaults.conf` forem alterados:

```bash
docker compose -f infra/docker/docker-compose.yml build spark-master spark-worker
docker compose --env-file .env -f infra/docker/docker-compose.yml up -d spark-master spark-worker
```

---

## Observações

* Os containers Spark precisam estar na rede `data-net` para alcançar o MinIO
* As credenciais MinIO são passadas como variáveis de ambiente - nunca hardcode no script
* O `spark-defaults.conf` configura S3A globalmente; as credenciais ficam no job via `os.getenv()`
* A UI do master (`http://localhost:8085`) mostra jobs em execução, workers registrados e histórico de aplicações
* Use sempre `s3a://` nos paths - o scheme `s3://` não é reconhecido pelo Hadoop S3A e causa `UnsupportedFileSystemException`
* A imagem Spark usa **Python 3.11** (multi-stage build sobre `apache/spark:3.5.3`); a versão oficial `python3` da imagem base é 3.8 e não é compatível com a sintaxe de type hints do projeto
