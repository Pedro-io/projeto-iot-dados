# Projeto IoT Lakehouse - Plataforma de Integração de Dados

Plataforma de integração de dados para monitoramento industrial, desenvolvida como Projeto Integrador da disciplina **Integração de Dados II (2026/1)** - PUC Minas.

O sistema ingere eventos de sensores IoT em tempo real via Apache Kafka, valida os schemas via Confluent Schema Registry, persiste os dados brutos no MinIO com arquitetura **Medallion** (Bronze -> Silver -> Gold) e emite logs estruturados em JSON para observabilidade.

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
| Observabilidade | Grafana + Loki + Promtail |

---

## Estrutura do Repositório

```
projeto-iot-dados/
├── docker-compose.yml              # Stack de streaming (Kafka + Schema Registry)
├── .env.template                   # Template de variáveis de ambiente
├── pyproject.toml                  # Gerenciamento de dependências (Poetry)
├── scripts/
│   ├── start.sh                    # Sobe toda a infra (Linux/Mac)
│   ├── start.ps1                   # Sobe toda a infra (Windows/PowerShell)
├── docs/
│   ├── setup.md                    # Guia de instalação e execução
│   ├── streaming.md                # Infraestrutura Kafka (serviços, comandos)
│   ├── consumer-bronze.md          # Arquitetura do consumer, parâmetros, logs
│   ├── troubleshooting.md          # Solução de problemas e comandos úteis
│   ├── arquitetura.md              # ADRs e decisões arquiteturais
│   ├── modelo-dados.md             # Modelagem MongoDB
│   ├── observability.md            # Stack de logs - Grafana, Loki, Promtail
│   └── gaps-e-pendencias.md        # O que ainda não foi implementado
├── infra/
│   ├── docker/                     # Stack de storage (MongoDB, PostgreSQL, MinIO)
│   └── terraform/                  # Provisionamento dos buckets S3 no MinIO
└── src/
    ├── ingestao/
    │   └── sensor_simulator.py     # Produtor Kafka - simula sensores IoT
    └── streaming/                  # Consumer Bronze e componentes SOLID
```

---

## Quick Start

```bash
# 1. Clonar e instalar dependências Python
git clone https://github.com/Pedro-io/projeto-iot-dados.git
cd projeto-iot-dados
poetry install
```

**Linux/Mac - subir toda a infra com um comando:**
```bash
# Na primeira vez, libere a execução dos scripts:
chmod +x scripts/start.sh scripts/start_without_terraform.sh scripts/stop-all.sh

./scripts/start.sh
```

**Windows (PowerShell) - subir toda a infra com um comando:**
```powershell
.\scripts\start.ps1
```

> Na primeira execução, se o `.env` não existir, o script cria a partir do template e para para você preencher as credenciais.

> O script executa automaticamente o Terraform para provisionar os buckets no MinIO (`terraform init` + `terraform apply`) - **as credenciais são lidas do `.env`, não é necessário editar `terraform.tfvars`**. Certifique-se de ter o Terraform instalado. Detalhes: [infra/terraform/README.md](infra/terraform/README.md)

> No Windows pode ser necessário liberar execução de scripts antes:
> ```powershell
> Set-ExecutionPolicy -Scope CurrentUser RemoteSigned
> ```

Após subir, os serviços ficam disponíveis em:

| Serviço | URL |
|---|---|
| Grafana | http://localhost:3001 (admin/admin) |
| MinIO Console | http://localhost:9001 |
| Kafka UI | http://localhost:8080 |
| Mongo Express | http://localhost:8083 |
| Adminer | http://localhost:8082 |

**Rodar simulador + consumer (após a infra estar no ar):**
```bash
poetry run python src/ingestao/sensor_simulator.py --events-per-second 50
cd src && poetry run python -m streaming.consumer.bronze_consumer
```

Guia completo com verificações e modo dry-run: [docs/setup.md](docs/setup.md)

---

## Documentação

| Documento | Conteúdo |
|---|---|
| [docs/setup.md](docs/setup.md) | Instalação, execução passo a passo, verificações |
| [docs/streaming.md](docs/streaming.md) | Stack Kafka - serviços, portas, comandos |
| [docs/consumer-bronze.md](docs/consumer-bronze.md) | Arquitetura SOLID, particionamento, logs, parâmetros |
| [docs/troubleshooting.md](docs/troubleshooting.md) | Erros comuns e comandos úteis |
| [docs/arquitetura.md](docs/arquitetura.md) | ADRs e decisões arquiteturais |
| [docs/modelo-dados.md](docs/modelo-dados.md) | Modelagem MongoDB |
| [docs/observability.md](docs/observability.md) | Stack de logs - Grafana, Loki, Promtail |
| [infra/docker/README.md](infra/docker/README.md) | Stack Docker de storage |
| [infra/terraform/README.md](infra/terraform/README.md) | Terraform - buckets MinIO |

---

## Roadmap

- [ ] **E2 - Silver:** Deduplicação, limpeza e tipagem via PySpark
- [ ] **E2 - Gold:** Agregações horárias/diárias para dashboards
- [ ] **E3 - Observabilidade:** Métricas Prometheus + Grafana
- [ ] **E3 - CI/CD:** GitHub Actions com linting + testes + build Docker
- [ ] **E4 - Schema Evolution:** Suporte a múltiplas versões de schema

---

## Integridade Acadêmica

Uso de IA generativa como ferramenta de apoio ao desenvolvimento. O entendimento do código, das decisões de arquitetura e dos princípios aplicados é responsabilidade da equipe conforme política da disciplina.

---

*Projeto acadêmico - PUC Minas 2026/1*
