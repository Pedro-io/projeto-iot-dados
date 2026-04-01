## Setup

Para executar este projeto, é necessário ter as seguintes ferramentas instaladas:

* Docker
* Python 3.14.3
* Poetry
* Terraform

Certifique-se de que todas estão corretamente configuradas antes de prosseguir.

---

## Estrutura de configuração

* O arquivo `.env` deve estar localizado na raiz do projeto
* O `docker-compose.yml` está em `infra/docker`
* Os scripts de infraestrutura (Terraform) estão em `infra/terraform`
* As dependências Python são gerenciadas pelo Poetry

Para mais detalhes:

* Infraestrutura Docker: `infra/docker/README.md`
* Infraestrutura com Terraform: `infra/terraform/README.md`

---

## Passos via Terminal

### 1. Configurar variáveis de ambiente

Copie o arquivo de exemplo:

```bash
cp .env.example .env
```

Edite o arquivo `.env` conforme necessário.

---

### 2. Inicializar a infraestrutura (Docker)

Certifique-se de que o Docker está em execução e execute:

```bash
docker-compose --env-file .env -f infra/docker/docker-compose.yml up -d
```

Para detalhes sobre os serviços (MongoDB, Postgres, MinIO, etc.), consulte:

```
infra/docker/README.md
```

---

### 3. Provisionar recursos com Terraform

Acesse a pasta de infraestrutura:

```bash
cd infra/terraform
```

Inicialize o Terraform:

```bash
terraform init
```

Visualize o plano das alterações que serão aplicadas:

```bash
terraform plan
```

Aplique a configuração:

```bash
terraform apply
```

Para mais detalhes sobre variáveis e recursos provisionados:

```
infra/terraform/README.md
```

---

### 4. Configurar ambiente Python

Configure o Poetry para criar o ambiente virtual dentro do projeto:

```bash
poetry config virtualenvs.in-project true
```

Instale as dependências:

```bash
poetry install
```

---

### 5. Executar Lint

Para verificar o código do projeto:

```bash
poetry run ruff check .
```

Para formatar o código:

```bash
poetry run ruff format .
```

---

### 6. Executar código Python

Para rodar qualquer script do projeto:

```bash
poetry run python src/main.py
```

---

## Observações

* O ambiente virtual `.venv` será criado automaticamente pelo Poetry
* O Docker utiliza o arquivo `.env` da raiz através da flag `--env-file`
* O Terraform é responsável por provisionar recursos no MinIO (como buckets)
* MongoDB e PostgreSQL são inicializados com dados básicos via scripts em `mongo-init` e `postgres-init`
* Não é necessário ativar manualmente o ambiente virtual ao utilizar `poetry run`
