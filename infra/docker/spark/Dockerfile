FROM apache/spark:3.5.3 AS spark_base

FROM python:3.11-slim-bookworm

RUN apt-get update && apt-get install -y --no-install-recommends \
        openjdk-17-jre-headless \
        curl \
        procps \
    && rm -rf /var/lib/apt/lists/*

COPY --from=spark_base /opt/spark /opt/spark

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV SPARK_HOME=/opt/spark
ENV PATH=$SPARK_HOME/bin:$SPARK_HOME/sbin:$PATH
ENV PYSPARK_PYTHON=python3.11
ENV PYSPARK_DRIVER_PYTHON=python3.11

RUN pip install --no-cache-dir \
    delta-spark==3.2.0 \
    pydeequ \
    loguru \
    python-dateutil

# JARs: S3A (MinIO), Delta Lake, Deequ, MongoDB Connector + Driver, PostgreSQL JDBC
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

COPY spark-defaults.conf /opt/spark/conf/spark-defaults.conf

WORKDIR /jobs
