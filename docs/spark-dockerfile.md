## Dockerfile Spark

Este documento explica o arquivo `infra/docker/spark/Dockerfile` usado para construir a imagem Spark do projeto.

O objetivo desta imagem é criar um container Spark que:
1. execute PySpark com Python 3.11
2. tenha suporte a S3A para MinIO
3. carregue bibliotecas extras usadas pelo pipeline
4. inclua os conectores necessários para MongoDB e PostgreSQL

---

## 1. Visão geral

O Dockerfile usa um build multistage com duas imagens base:
1. `apache/spark:3.5.3` como fonte da instalação do Spark
2. `python:3.11-slim-bookworm` como imagem final do container

A ideia é copiar o Spark já instalado da imagem oficial para a imagem final que traz Python 3.11 e dependências adicionais.

---

## 2. Etapas do Dockerfile

### 2.1 Base do Spark

```dockerfile
FROM apache/spark:3.5.3 AS spark_base
```

Esta etapa cria uma imagem temporária chamada `spark_base` para usar o Spark pré-instalado.

---

### 2.2 Imagem final Python

```dockerfile
FROM python:3.11-slim-bookworm
```

A imagem final é baseada no Python 3.11 slim. Isso garante compatibilidade com os scripts do projeto e evita o uso da versão Python padrão da imagem Spark oficial.

---

### 2.3 Instalação de Java e utilitários

```dockerfile
RUN apt-get update && apt-get install -y --no-install-recommends \
        openjdk-17-jre-headless \
        curl \
        procps \
    && rm -rf /var/lib/apt/lists/*
```

Esta etapa instala:
- `openjdk-17-jre-headless` para executar o Spark
- `curl` para baixar JARs necessários
- `procps` para utilitários de processo

O comando também limpa o cache do apt para reduzir o tamanho da imagem.

---

### 2.4 Copiar o Spark da imagem base

```dockerfile
COPY --from=spark_base /opt/spark /opt/spark
```

Aqui o diretório do Spark do primeiro estágio é copiado para a imagem final. Isso traz todos os binários e bibliotecas do Spark.

---

### 2.5 Variáveis de ambiente

```dockerfile
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV SPARK_HOME=/opt/spark
ENV PATH=$SPARK_HOME/bin:$SPARK_HOME/sbin:$PATH
ENV PYSPARK_PYTHON=python3.11
ENV PYSPARK_DRIVER_PYTHON=python3.11
```

Essas variáveis definem:
- `JAVA_HOME` para o Java instalado
- `SPARK_HOME` para o diretório onde o Spark foi copiado
- `PATH` para permitir execução dos comandos Spark
- `PYSPARK_PYTHON` e `PYSPARK_DRIVER_PYTHON` para garantir que o Spark use Python 3.11 no executor e no driver

---

### 2.6 Pacotes Python adicionais

```dockerfile
RUN pip install --no-cache-dir \
    delta-spark==3.2.0 \
    pydeequ \
    loguru \
    python-dateutil
```

Os pacotes instalados são:
- `delta-spark` para suporte ao Delta Lake
- `pydeequ` para regras de qualidade de dados
- `loguru` para logging mais simples nos scripts Python
- `python-dateutil` para manipulação de datas

O `--no-cache-dir` evita armazenar arquivos temporários do pip na imagem.

---

### 2.7 Download de JARs extras

Esta etapa baixa bibliotecas Java necessárias para integração com serviços externos.

```dockerfile
RUN curl -fL -o /opt/spark/jars/hadoop-aws-3.3.4.jar \
        "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar" && \
    curl -fL -o /opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar \
        "https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar" && \
    curl -fL -o /opt/spark/jars/delta-spark_2.12-3.2.0.jar \
        "https://repo1.maven.org/maven2/io/delta/delta-spark_2.12/3.2.0/delta-spark_2.12-3.2.0.jar" && \
    curl -fL -o /opt/spark/jars/delta-storage-3.2.0.jar \
        "https://repo1.maven.org/maven2/io/delta/delta-storage/3.2.0/delta-storage-3.2.0.jar" && \
    curl -fL -o /opt/spark/jars/deequ-2.0.9-spark-3.5.jar \
        "https://repo1.maven.org/maven2/com/amazon/deequ/deequ/2.0.9-spark-3.5/deequ-2.0.9-spark-3.5.jar" && \
    curl -fL -o /opt/spark/jars/mongo-spark-connector_2.12-10.4.0.jar \
        "https://repo1.maven.org/maven2/org/mongodb/spark/mongo-spark-connector_2.12/10.4.0/mongo-spark-connector_2.12-10.4.0.jar" && \
    curl -fL -o /opt/spark/jars/bson-4.11.1.jar \
        "https://repo1.maven.org/maven2/org/mongodb/bson/4.11.1/bson-4.11.1.jar" && \
    curl -fL -o /opt/spark/jars/mongodb-driver-core-4.11.1.jar \
        "https://repo1.maven.org/maven2/org/mongodb/mongodb-driver-core/4.11.1/mongodb-driver-core-4.11.1.jar" && \
    curl -fL -o /opt/spark/jars/mongodb-driver-sync-4.11.1.jar \
        "https://repo1.maven.org/maven2/org/mongodb/mongodb-driver-sync/4.11.1/mongodb-driver-sync-4.11.1.jar" && \
    curl -fL -o /opt/spark/jars/postgresql-42.7.3.jar \
        "https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.3/postgresql-42.7.3.jar"
```

Os JARs adicionados são:
- `hadoop-aws` e `aws-java-sdk-bundle` para habilitar S3A
- `delta-spark` e `delta-storage` para Delta Lake
- `deequ` para qualidade de dados
- `mongo-spark-connector`, `bson`, `mongodb-driver-core` e `mongodb-driver-sync` para acessar MongoDB
- `postgresql` para acesso JDBC ao PostgreSQL

Esses arquivos vão para `/opt/spark/jars`, onde o Spark já os carrega automaticamente.

---

### 2.8 Cópia do spark-defaults.conf

```dockerfile
COPY spark-defaults.conf /opt/spark/conf/spark-defaults.conf
```

O arquivo `spark-defaults.conf` contém configurações globais do Spark, incluindo as propriedades S3A usadas para conectar ao MinIO. Ele é aplicado automaticamente em todas as execuções do Spark dentro do container.

---

### 2.9 Definir diretório de trabalho

```dockerfile
WORKDIR /jobs
```

O diretório de trabalho é definido para `/jobs`. No `docker-compose.yml`, a pasta `src/` do projeto é montada nesse diretório. Assim, os scripts Python ficam disponíveis em `/jobs` dentro do container.

---

## 3. Como usar essa imagem

No `infra/docker/docker-compose.yml`, os serviços `spark-master` e `spark-worker` usam esta imagem customizada.

Quando o arquivo `infra/docker/spark/Dockerfile` ou `infra/docker/spark/spark-defaults.conf` for alterado, é necessário rebuildar a imagem Spark com:

```bash
docker compose -f infra/docker/docker-compose.yml build spark-master spark-worker
```

Em seguida suba os containers com:

```bash
docker compose --env-file .env -f infra/docker/docker-compose.yml up -d spark-master spark-worker
```

---

## 4. Por que este Dockerfile é importante

A imagem Spark padrão não inclui Python 3.11 nem os conectores e bibliotecas extras usados pelo projeto.

Este Dockerfile garante que a imagem:
- rode PySpark com a versão correta de Python
- tenha as dependências do projeto instaladas via pip
- tenha suporte a S3A para MinIO
- tenha conectores para MongoDB e PostgreSQL
- carregue configurações globais do Spark via `spark-defaults.conf`

---

## 5. Relação com outros arquivos do projeto

- `infra/docker/docker-compose.yml`
  - define os serviços Spark e monta o código do projeto em `/jobs`
- `infra/docker/spark/spark-defaults.conf`
  - contém as configurações S3A e outras propriedades do Spark
- `docs/spark-jobs.md`
  - explica como rodar jobs Spark no cluster local
- `docs/setup.md`
  - descreve a instalação do ambiente e os passos de inicialização
