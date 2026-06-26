# Scripts de Inicialização e Parada

Este diretório contém scripts para subir e parar partes específicas da stack, para reduzir o uso de recursos da máquina.

## Infraestrutura base

- Bash:
  - `./start-infra.sh` - sobe MongoDB, MinIO, Postgres, Loki, Promtail e Grafana
  - `./stop-infra.sh` - para a infraestrutura base

- PowerShell:
  - `.\start-infra.ps1`
  - `.\stop-infra.ps1`

## Kafka

- Bash:
  - `./start-kafka.sh` - sobe Kafka, Schema Registry e Kafka UI
  - `./stop-kafka.sh` - para Kafka, Schema Registry e Kafka UI

- PowerShell:
  - `.\start-kafka.ps1`
  - `.\stop-kafka.ps1`

## Airflow

- Bash:
  - `./start-airflow.sh` - inicializa o banco do Airflow e sobe scheduler, worker e API
  - `./stop-airflow.sh` - para o Airflow completo

- PowerShell:
  - `.\start-airflow.ps1`
  - `.\stop-airflow.ps1`

## Observações

- Os scripts de Airflow dependem de um arquivo `.env` no diretório raiz.
- O script `start-infra` usa `infra/docker/docker-compose.yml`.
- Os scripts de Kafka usam o arquivo `infra/kafka/docker-compose.yml`.
- Use `docker compose ps` para verificar o status dos containers após subir os serviços.
