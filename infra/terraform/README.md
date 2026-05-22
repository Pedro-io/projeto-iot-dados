## Terraform - Infraestrutura

Este diretório contém a configuração de infraestrutura utilizando Terraform.

Atualmente, o Terraform é utilizado para provisionar recursos no MinIO (compatível com S3), como buckets para armazenamento de dados.

---

## Automação via Scripts

**⚠️ IMPORTANTE:** Não execute Terraform manualmente diretamente aqui!

Os scripts `start.sh` (Linux/Mac) e `start.ps1` (Windows) automaticamente:

1. Leem as credenciais do `.env` na raiz do projeto
2. Executam `terraform init`
3. Executam `terraform apply` passando as credenciais via `-var`

Execute a partir da **raiz do projeto**:

```bash
# Linux/Mac
./scripts/start.sh

# Windows (PowerShell)
.\scripts\start.ps1
```

---

## Pré-requisitos

* Terraform instalado
* Docker em execução (MinIO deve estar ativo)
* `.env` configurado com credenciais MinIO

---

## Variáveis

As credenciais são **lidas automaticamente do `.env` pelos scripts**.

O arquivo `terraform.tfvars` contém valores padrão apenas para referência — é **override** pelos scripts via `-var`.

**Variáveis obrigatórias no `.env`:**

```env
MINIO_ENDPOINT=http://localhost:9000
MINIO_ROOT_USER=1234
MINIO_ROOT_PASSWORD=1234
```

---

## Execução Manual (se necessário)

Se precisar executar Terraform manualmente:

```bash
cd infra/terraform
terraform init
terraform apply \
  -var="minio_endpoint=http://localhost:9000" \
  -var="minio_access_key=1234" \
  -var="minio_secret_key=1234" \
  -auto-approve
```

**IMPORTANTE:** Os valores `minio_access_key` e `minio_secret_key` devem corresponder a `MINIO_ROOT_USER` e `MINIO_ROOT_PASSWORD` do `.env`.

---

## Comandos básicos

### Inicializar

```
terraform init
```

---

### Validar

```
terraform validate
```

---

### Planejar

```
terraform plan
```

---

### Aplicar

```
terraform apply
```

---

### Destruir

```
terraform destroy
```

---

## Recursos criados

* Buckets S3 no MinIO:

  * raw-data
  * processed-data

---

## Observações

* O MinIO deve estar rodando antes de aplicar o Terraform
* O endpoint configurado deve ser acessível (ex: http://localhost:9000)
* As credenciais devem ser consistentes com o ambiente Docker
