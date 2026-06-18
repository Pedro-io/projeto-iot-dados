## Streaming - Infraestrutura Kafka

Este documento descreve a stack de streaming do projeto, responsável por receber e rotear eventos dos sensores IoT em tempo real.

A stack é definida no arquivo `docker-compose.yml` na raiz do projeto.

---

## Serviços disponíveis

### Apache Kafka (KRaft)

Broker de mensagens que recebe os eventos dos sensores IoT.

* Porta externa: `9092`
* Tópico principal: `iot-sensors-raw`
* Modo: KRaft (sem Zookeeper)

Na primeira subida, a imagem formata automaticamente o storage usando o `CLUSTER_ID` definido no compose - nenhuma ação manual é necessária.

---

### Confluent Schema Registry

Serviço de validação e versionamento de schemas JSON.

* URL: `http://localhost:8081`

O schema do tópico `iot-sensors-raw` é registrado automaticamente pelo serviço `schema-registry-init` na inicialização. Após concluir, o container encerra com `Exited (0)` - comportamento esperado.

Para verificar se o schema foi registrado:

```bash
curl http://localhost:8081/subjects
# Resposta esperada: ["iot-sensors-raw-value"]
```

Para inspecionar o schema completo:

```bash
curl http://localhost:8081/subjects/iot-sensors-raw-value/versions/latest
```

> **Atenção:** Schema Registry e Mongo Express usam a mesma porta `8081`. Não suba as duas stacks ao mesmo tempo sem ajustar as portas.

---

### Kafka UI

Interface web para inspecionar tópicos, partições, consumer groups e schemas.

* URL: `http://localhost:8080`

---

## Fluxo de dados

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
  MinIO - Camada Bronze
  factory_id=.../measurement_type=.../dt=.../batch_xxx.ndjson
```

---

## Comandos básicos

### Subir a stack de streaming

Execute a partir da raiz do projeto:

```bash
docker compose up -d
```

### Verificar status

```bash
docker compose ps
```

Saída esperada:

```
NAME                   STATUS
kafka                  Up (healthy)
schema-registry        Up (healthy)
schema-registry-init   Exited (0)    ← normal, encerra após registrar o schema
kafka-ui               Up
```

### Acompanhar logs

```bash
# Todos os serviços
docker compose logs -f

# Apenas Kafka
docker compose logs -f kafka

# Últimas 10 linhas do Schema Registry
docker compose logs schema-registry --tail=10
```

### Parar sem apagar dados

```bash
docker compose down
```

### Parar e resetar volumes Kafka (reset completo)

```bash
docker compose down -v
```

---

## Observações

* O Kafka usa KRaft - não há dependência de Zookeeper
* O Schema Registry deve estar saudável antes de rodar o consumer
* Se o Schema Registry estiver offline, o consumer usa validação local como fallback automático
* A stack de storage (MinIO, MongoDB, PostgreSQL) é separada - ver [infra/docker/README.md](../infra/docker/README.md)
