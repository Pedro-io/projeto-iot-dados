# Projeto IoT Lakehouse — Plataforma de Integração de Dados

Plataforma de integração de dados para monitoramento industrial, desenvolvida como Projeto Integrador da disciplina **Integração de Dados II (2026/1)** — PUC Minas.

O sistema ingere eventos de sensores IoT em tempo real via Apache Kafka, valida os schemas via Confluent Schema Registry, persiste os dados brutos no MinIO com arquitetura **Medallion** (Bronze → Silver → Gold) e emite logs estruturados em JSON para observabilidade.

---

## Arquitetura Medallion

| Camada | Descrição | Status |
|---|---|---|
| Bronze | Dados brutos em JSON Lines, append-only, particionados por fábrica, tipo e data | ✅ E1 |
| Silver | Dados limpos, tipados e deduplicados via Spark/Flink | 🔄 E2 |
| Gold | Agregações horárias e diárias para consumo analítico | 🔄 E2/E3 |

Decisões de arquitetura e ADRs: [docs/arquitetura.md](docs/arquitetura.md)

---

## Stack Tecnológica

| Categoria | Tecnologia |
|---|---|
| Containerização | Docker + Docker Compose |
| Streaming | Apache Kafka (KRaft) + Confluent Schema Registry |
| Object Storage | MinIO (compatível com S3) |
| Banco de Dados | MongoDB + PostgreSQL |
| IaC | Terraform |
| Linguagem | Python 3.10+ (Poetry) |

---

## Estrutura do Repositório

```
projeto-iot-dados/
├── docker-compose.yml              # Stack de streaming (Kafka + Schema Registry)
├── .env.template                   # Template de variáveis de ambiente
├── pyproject.toml                  # Gerenciamento de dependências (Poetry)
├── docs/
│   ├── setup.md                    # Guia de instalação e execução
│   ├── streaming.md                # Infraestrutura Kafka (serviços, comandos)
│   ├── consumer-bronze.md          # Arquitetura do consumer, parâmetros, logs
│   ├── troubleshooting.md          # Solução de problemas e comandos úteis
│   ├── arquitetura.md              # ADRs e decisões arquiteturais
│   ├── modelo-dados.md             # Modelagem MongoDB
│   └── gaps-e-pendencias.md        # O que ainda não foi implementado
├── infra/
│   ├── docker/                     # Stack de storage (MongoDB, PostgreSQL, MinIO)
│   └── terraform/                  # Provisionamento dos buckets S3 no MinIO
└── src/
    ├── ingestao/
    │   └── sensor_simulator.py     # Produtor Kafka — simula sensores IoT
    └── streaming/                  # Consumer Bronze e componentes SOLID
```

---

## Quick Start

```bash
# 1. Clonar e configurar ambiente
git clone https://github.com/Pedro-io/projeto-iot-dados.git
cd projeto-iot-dados
cp .env.template .env
poetry install

# 2. Subir storage
docker compose --env-file .env -f infra/docker/docker-compose.yml up -d

# 3. Provisionar buckets no MinIO
cd infra/terraform && terraform init && terraform apply -auto-approve && cd ../..

# 4. Subir streaming
docker compose up -d

# 5. Rodar simulador + consumer
poetry run python src/ingestao/sensor_simulator.py --events-per-second 50
cd src && poetry run python -m streaming.consumer.bronze_consumer
```

Guia completo com verificações e modo dry-run: [docs/setup.md](docs/setup.md)

---

## Documentação

| Documento | Conteúdo |
|---|---|
| [docs/setup.md](docs/setup.md) | Instalação, execução passo a passo, verificações |
| [docs/streaming.md](docs/streaming.md) | Stack Kafka — serviços, portas, comandos |
| [docs/consumer-bronze.md](docs/consumer-bronze.md) | Arquitetura SOLID, particionamento, logs, parâmetros |
| [docs/troubleshooting.md](docs/troubleshooting.md) | Erros comuns e comandos úteis |
| [docs/arquitetura.md](docs/arquitetura.md) | ADRs e decisões arquiteturais |
| [docs/modelo-dados.md](docs/modelo-dados.md) | Modelagem MongoDB |
| [infra/docker/README.md](infra/docker/README.md) | Stack Docker de storage |
| [infra/terraform/README.md](infra/terraform/README.md) | Terraform — buckets MinIO |

---

## Roadmap

- [ ] **E2 — Silver:** Deduplicação, limpeza e tipagem via PySpark
- [ ] **E2 — Gold:** Agregações horárias/diárias para dashboards
- [ ] **E3 — Observabilidade:** Métricas Prometheus + Grafana
- [ ] **E3 — CI/CD:** GitHub Actions com linting + testes + build Docker
- [ ] **E4 — Schema Evolution:** Suporte a múltiplas versões de schema

---

## Integridade Acadêmica

Uso de IA generativa como ferramenta de apoio ao desenvolvimento. O entendimento do código, das decisões de arquitetura e dos princípios aplicados é responsabilidade da equipe conforme política da disciplina.

---

## Equipe

| Nome | Matrícula | Responsabilidade |
|---|---|---|
| — | — | Arquitetura e camada Bronze |
| — | — | Infraestrutura Docker/Terraform |
| — | — | Testes e documentação |

---

*Projeto acadêmico — PUC Minas 2026/1*
