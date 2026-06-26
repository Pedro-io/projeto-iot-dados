from datetime import datetime

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import TaskGroup

ENVIRONMENT = "prd"
OWNER = "airflow"

# Monta o prefixo do spark-submit via docker exec no container spark-master
_SPARK_SUBMIT = (
    "docker exec "
    "-e MINIO_ROOT_USER=$MINIO_ROOT_USER "
    "-e MINIO_ROOT_PASSWORD=$MINIO_ROOT_PASSWORD "
    "-e PYTHONPATH=/jobs "
    "spark-master "
    "/opt/spark/bin/spark-submit "
    "--master spark://spark-master:7077 "
)

_COMMON = f"--execution_date {{{{ ds }}}} --environment {ENVIRONMENT} --owner {OWNER}"

_JDBC = (
    "--jdbc_url jdbc:postgresql://postgres:5432/$POSTGRES_DB "
    "--jdbc_user $POSTGRES_USER "
    "--jdbc_password $POSTGRES_PASSWORD"
)

_MONGO = (
    "--mongo_uri mongodb://$MONGO_USER:$MONGO_PASS@mongodb:27017 "
    "--mongo_database Database "
    "--mongo_collection equipments"
)


def spark_job(script_path: str, extra_args: str = "") -> str:
    cmd = f"{_SPARK_SUBMIT}/jobs/{script_path} {_COMMON}"
    if extra_args:
        cmd += f" {extra_args}"
    return cmd


default_args = {
    "owner": OWNER,
    "retries": 0,
}

with DAG(
    dag_id="pipeline_iot",
    description="Pipeline IoT batch diário: Bronze -> Silver -> Gold",
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["iot", "bronze", "silver", "gold"],
    max_active_runs=1,
    max_active_tasks=1,
) as dag:

    
    # BRONZE - ingestão batch de todas as fontes em paralelo
    
    with TaskGroup("bronze") as bronze:
        pg_tables = [
            "fabricas",
            "equipamentos",
            "sensores",
            "tipos_equipamento",
            "tipos_medicao",
            "status_qualidade",
            "leituras",
            "metadados_leitura",
        ]

        for table in pg_tables:
            BashOperator(
                task_id=f"postgres_{table}",
                bash_command=spark_job(
                    f"processamento/bronze/postgres/{table}.py", _JDBC
                ),
            )

        BashOperator(
            task_id="mongo_equipamentos",
            bash_command=spark_job(
                "processamento/bronze/mongo/equipamentos.py", _MONGO
            ),
        )

    
    # SILVER - limpeza, tipagem e deduplicação (paralelo, após Bronze)
    
    with TaskGroup("silver") as silver:
        silver_scripts = [
            "postgres/fabricas",
            "postgres/equipamentos",
            "postgres/sensores",
            "postgres/tipos_equipamento",
            "postgres/tipos_medicao",
            "postgres/status_qualidade",
            "postgres/leituras",
            "postgres/metadados_leitura",
            "mongo/equipamentos",
            "kafka/sensor_events",
        ]

        for script in silver_scripts:
            BashOperator(
                task_id=script.replace("/", "_"),
                bash_command=spark_job(f"processamento/silver/{script}.py"),
            )

    
    # GOLD - dimensões em paralelo, depois fatos
    
    with TaskGroup("gold") as gold:
        with TaskGroup("dimensions") as dims:
            for dim in [
                "dim_fabrica",
                "dim_equipamento",
                "dim_sensor",
                "dim_tipo_medicao",
                "dim_tempo",
            ]:
                BashOperator(
                    task_id=dim,
                    bash_command=spark_job(
                        f"processamento/gold/dimensions/{dim}.py"
                    ),
                )

        with TaskGroup("facts") as facts:
            BashOperator(
                task_id="fct_leituras_hora",
                bash_command=spark_job(
                    "processamento/gold/facts/fct_leituras_hora.py"
                ),
            )
            BashOperator(
                task_id="fct_anomalias_dia",
                bash_command=spark_job(
                    "processamento/gold/facts/fct_anomalias_dia.py"
                ),
            )

        dims >> facts

    bronze >> silver >> gold