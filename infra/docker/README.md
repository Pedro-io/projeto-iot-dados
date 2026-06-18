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

### Spark Master

Gerenciador do cluster Spark. Recebe os jobs submetidos e distribui tarefas para os workers.

* Spark Master UI: http://localhost:8085
* Endpoint do cluster: `spark://localhost:7077`

---

### Spark Worker

Executor do cluster. Recebe tarefas do master e processa os dados.

* Memória disponível: 2G
* Cores disponíveis: 2

O worker aparece como registrado na UI do master (`http://localhost:8085`) alguns segundos após subir.

---

### Conectividade do Spark

Os containers Spark estão configurados para acessar o MinIO via protocolo S3A. A configuração fica em:

```bash
spark/spark-defaults.conf
```

As credenciais (`MINIO_ROOT_USER` e `MINIO_ROOT_PASSWORD`) são passadas via variáveis de ambiente e devem ser lidas nos jobs via `os.getenv()`.

Os jobs ficam em `src/` e são montados em `/jobs` dentro dos containers.

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

Na primeira execução, o Spark faz o build da imagem customizada (download dos jars S3A). As execuções seguintes usam o cache do Docker e são imediatas.

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

### Submeter um job Spark

```bash
docker exec spark-master spark-submit \
  --master spark://spark-master:7077 \
  /jobs/<caminho-do-script>.py
```

Exemplo com o job de validação de conexão ao MinIO:

```bash
docker exec spark-master spark-submit \
  --master spark://spark-master:7077 \
  /jobs/processamento/test_spark_conexao.py
```

---

### Fazer rebuild da imagem Spark

Necessário quando o `Dockerfile` ou o `spark-defaults.conf` forem alterados:

```bash
docker-compose -f infra/docker/docker-compose.yml build spark-master spark-worker
docker-compose -f infra/docker/docker-compose.yml up -d spark-master spark-worker
```

---

## Observações

* O arquivo `.env` deve estar na raiz do projeto
* Os serviços MongoDB e PostgreSQL são inicializados automaticamente via scripts nas pastas `mongo-init` e `postgres-init`
* O MinIO é utilizado como armazenamento S3 local
* A criação de buckets no MinIO é feita via Terraform
* Os containers utilizam redes internas (`data-net` e `ui-net`) para comunicação
* O Spark acessa o MinIO via S3A - as credenciais devem estar no `.env` e são passadas como variáveis de ambiente para os containers

---

## Fluxo recomendado

1. Subir a infraestrutura com Docker
2. Executar o Terraform para provisionar buckets no MinIO
3. Verificar se o worker Spark está registrado em http://localhost:8085
4. Utilizar o ambiente para ingestão e processamento de dados
