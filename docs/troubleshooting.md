## Troubleshooting e Comandos Úteis

---

## Solução de Problemas

| Erro | Causa | Solução |
|---|---|---|
| `kafka is unhealthy` | Kafka ainda inicializando (KRaft formata storage na 1ª subida) | Aguarde 30s e rode `docker compose -f infra/kafka/docker-compose.yml ps` |
| `NoBrokersAvailable` | Kafka não acessível | Verifique se `docker compose -f infra/kafka/docker-compose.yml ps` mostra Kafka `Up (healthy)` |
| `ERRO: Variável 'KAFKA_BOOTSTRAP_SERVERS' não definida` | Arquivo `.env` não existe ou está incompleto | Copie `.env.template` para `.env` e preencha |
| `python-dotenv não instalado` | Dependências não instaladas | Execute `poetry install` |
| `docker não reconhecido` | Docker Desktop fechado | Abra o Docker Desktop e aguarde a baleia estabilizar |
| Schema Registry retorna `404` | Schema ainda não registrado ou Registry offline | Consumer usa validação local como fallback automaticamente |
| Bucket `bronze` vazio | Consumer rodou com `--dry-run` | Rode sem a flag `--dry-run` |
| Bucket `bronze` não existe | Terraform não foi aplicado | Execute `cd infra/terraform && terraform apply` |
| Conflito de porta 8081 | Mongo Express e Schema Registry usam a mesma porta | Suba apenas uma stack por vez, ou ajuste a porta do Mongo Express |
| VS Code não reconhece `docker` ou `pip` | Ferramentas instaladas com VS Code aberto | Feche e reabra o VS Code após instalar |
| Testes falhando com `ImportError` | PYTHONPATH não configurado | Execute `PYTHONPATH=src python -m unittest discover` |

---

## Comandos Úteis

```bash
# --- Streaming (Kafka) ---
docker compose -f infra/kafka/docker-compose.yml ps
docker compose -f infra/kafka/docker-compose.yml logs -f kafka
docker compose -f infra/kafka/docker-compose.yml logs schema-registry --tail=10
docker compose -f infra/kafka/docker-compose.yml down
docker compose -f infra/kafka/docker-compose.yml down -v   # reset completo (apaga volumes Kafka)

# --- Storage (MongoDB, PostgreSQL, MinIO) ---
docker compose -f infra/docker/docker-compose.yml ps
docker compose -f infra/docker/docker-compose.yml down

# --- Schema Registry ---
curl http://localhost:8081/subjects
curl http://localhost:8081/subjects/iot-sensors-raw-value/versions/latest

# --- MinIO ---
docker exec -it minio mc ls --recursive local/bronze

# --- Testes e qualidade ---
PYTHONPATH=src python -m unittest discover -s tests -v
poetry run ruff check .
poetry run ruff format .

# --- Verificar configuração carregada do .env ---
cd src
python -c "from streaming.config import DEFAULT_MINIO_ENDPOINT, DEFAULT_BUCKET; print(f'MinIO: {DEFAULT_MINIO_ENDPOINT}'); print(f'Bucket: {DEFAULT_BUCKET}')"

# --- Modo dry-run (testes sem infra) ---
python src/ingestao/sensor_simulator.py --dry-run --events-per-second 5
cd src && python -m streaming.consumer.bronze_consumer --dry-run --flush-size 10 --flush-interval 5
```
