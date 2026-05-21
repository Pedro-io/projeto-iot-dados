## Docker - Infraestrutura Local

Este diretório contém a definição da infraestrutura local do projeto utilizando Docker Compose.

Os serviços aqui definidos simulam os principais componentes de uma plataforma de dados, incluindo banco de dados, armazenamento e interfaces de administração.

---

## Serviços disponíveis

### MongoDB

Banco de dados NoSQL utilizado para ingestão e armazenamento de dados.

* Porta interna: 27017
* Inicialização automática via scripts em:

```bash
mongo-init/
```

Esses scripts são executados automaticamente na criação do container, permitindo configurar coleções, índices ou dados iniciais.

---

### Mongo Express

Interface web para gerenciamento do MongoDB.

* URL: http://localhost:8083

---

### PostgreSQL

Banco de dados relacional utilizado para armazenamento estruturado.

* Porta interna: 5432
* Inicialização automática via scripts em:

```bash
postgres-init/
```

Esses scripts permitem criar schemas, tabelas e dados iniciais no momento da subida do container.

---

### Adminer

Interface web para gerenciamento do PostgreSQL.

* URL: http://localhost:8082

---

### MinIO

Armazenamento de objetos compatível com S3.

* API S3: http://localhost:9000
* Console Web: http://localhost:9001

O MinIO é utilizado como camada de armazenamento de dados (data lake).

Importante:

* Os buckets do MinIO não são criados via Docker
* O provisionamento é feito utilizando o Terraform localizado em:

```bash
infra/terraform/
```

---

## Variáveis de ambiente

Todas as variáveis estão centralizadas no arquivo `.env` na raiz do projeto.

Exemplo:

```bash
MONGO_USER=admin
MONGO_PASS=123

ME_USER=admin
ME_PASS=123

POSTGRES_USER=postgres
POSTGRES_PASSWORD=123
POSTGRES_DB=postgres

MINIO_ROOT_USER=minioadmin
MINIO_ROOT_PASSWORD=123
```

---

## Comandos básicos

### Subir a infraestrutura

Execute a partir da raiz do projeto:

```bash
docker-compose --env-file .env -f infra/docker/docker-compose.yml up -d
```

---

### Parar a infraestrutura

```bash
docker-compose -f infra/docker/docker-compose.yml down
```

---

### Visualizar logs

```bash
docker-compose -f infra/docker/docker-compose.yml logs -f
```

---

## Observações

* O arquivo `.env` deve estar na raiz do projeto
* Os serviços MongoDB e PostgreSQL são inicializados automaticamente via scripts nas pastas `mongo-init` e `postgres-init`
* O MinIO é utilizado como armazenamento S3 local
* A criação de buckets no MinIO é feita via Terraform
* Os containers utilizam redes internas (`data-net` e `ui-net`) para comunicação

---

## Fluxo recomendado

1. Subir a infraestrutura com Docker
2. Executar o Terraform para provisionar buckets no MinIO
3. Utilizar o ambiente para ingestão e processamento de dados
