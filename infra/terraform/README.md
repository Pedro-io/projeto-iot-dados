## Terraform - Infraestrutura

Este diretório contém a configuração de infraestrutura utilizando Terraform.

Atualmente, o Terraform é utilizado para provisionar recursos no MinIO (compatível com S3), como buckets para armazenamento de dados.

---

## Pré-requisitos

* Terraform instalado
* Docker em execução (MinIO deve estar ativo)

---

## Variáveis

As variáveis estão definidas no arquivo:

```
terraform.tfvars
```

Exemplo:

```
minio_endpoint   = "http://localhost:9000"
minio_access_key = "minioadmin"
minio_secret_key = "minioadmin"
```

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
