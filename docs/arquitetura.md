# Arquitetura — Plataforma de Integração de Dados IoT

## Visão Geral

A plataforma implementa uma **arquitetura Medallion** (Bronze → Silver → Gold) sobre object storage compatível com S3 (MinIO), orquestrada por Apache Airflow e processada via Apache Spark com Delta Lake.

```
┌─────────────────────────────────────────────────────────────────────┐
│                        FONTES DE DADOS                              │
│  Sensores IoT       PostgreSQL ERP      MongoDB             API REST │
│  (Simulador)        (erp_legado)        (equipments)        (—)     │
└──────┬──────────────────┬───────────────────┬──────────────────┬────┘
       │  Streaming        │  Batch JDBC        │  Batch JDBC      │ Batch
       ▼                   ▼                   ▼                  ▼
┌─────────────────────────────────────────────────────────────────────┐
│                     CAMADA DE INGESTÃO                              │
│  Apache Kafka (KRaft)           spark-submit (AbstractETL)          │
│  consumer → NDJSON              full load / incremental             │
└──────┬──────────────────────────────────┬───────────────────────────┘
       │                                  │
       ▼                                  ▼
┌─────────────────────────────────────────────────────────────────────┐
│              BRONZE  —  MinIO bucket: bronze                        │
│  factory_id=*/measurement_type=*/dt=*/*.ndjson  (Kafka, append)     │
│  postgres/{tabela}/                             (Parquet, append)   │
│  mongo/equipamentos/                            (Parquet, append)   │
└──────────────────────────────┬──────────────────────────────────────┘
                               │  spark-submit (SilverETL)
                               ▼
┌─────────────────────────────────────────────────────────────────────┐
│              SILVER  —  MinIO bucket: silver                        │
│  kafka/sensor_events/        (Delta, upsert, part. measurement_type)│
│  postgres/{7 tabelas}/       (Delta, upsert, full ou incremental)   │
│  mongo/equipamentos/         (Delta, upsert, full load)             │
└──────────────────────────────┬──────────────────────────────────────┘
                               │  spark-submit (GoldETL) [planejado]
                               ▼
┌─────────────────────────────────────────────────────────────────────┐
│              GOLD  —  MinIO bucket: gold  [planejado]               │
│  fct_leituras_hora/          (Delta, agregações horárias)           │
│  fct_anomalias_dia/          (Delta, contagem diária de anomalias)  │
│  dim_{tempo,fabrica,equipamento,sensor,tipo_medicao}/               │
└──────────────────────────────┬──────────────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────────────┐
│                   CAMADA DE ORQUESTRAÇÃO  [planejado]               │
│  Apache Airflow — DAGs: bronze_daily, silver_daily, gold_daily      │
└─────────────────────────────────────────────────────────────────────┘
```

## Status de Implementação (2026-06-19)

| Camada   | Status       | Tabelas                                                       |
|----------|--------------|---------------------------------------------------------------|
| Bronze   | ✅ Completo  | 8 Parquet (PG + Mongo) + NDJSON Kafka particionado            |
| Silver   | ✅ Completo  | 10 Delta Tables (PG×8, Mongo×1, Kafka×1)                      |
| Gold     | 🔲 Planejado | Star schema dimensional (ver `docs/catalogo-dados.md`)        |
| Airflow  | 🔲 Planejado | DAGs Bronze/Silver/Gold com schedule diário                   |

---

# Decisões Arquiteturais

Este documento centraliza os registros de decisões arquiteturais (*Architecture Decision Records* - ADRs) do projeto Plataforma de Integração de Dados IoT. Cada ADR descreve o contexto, a decisão tomada, as alternativas consideradas e as consequências resultantes.

---

# ADR-001: Banco de Dados NoSQL para Cadastro Mestre de Equipamentos

## Status
Aceito

## Contexto
É necessário armazenar o cadastro mestre dos equipamentos industriais. Esses equipamentos possuem estrutura hierárquica (fábrica, equipamento, múltiplos sensores) e evolutiva, com sensores variando em quantidade, tipo e limiares operacionais (`range`).

O pipeline de streaming precisa resolver os metadados de cada evento recebido pelo Kafka de forma atômica e com baixa latência. Qualquer solução que exija múltiplas consultas ou joins em tempo real compromete o throughput do consumidor.

## Decisão
Utilizar o **MongoDB** (banco de dados NoSQL orientado a documentos) como repositório de Master Data para o cadastro de equipamentos, adotando a estratégia de **Documentos Embutidos** (*Embedding Documents*). Os atributos da fábrica e o arranjo de sensores são embutidos diretamente no documento-pai do Equipamento. Índices secundários do tipo *Multikey Index* garantem eficiência em consultas por `sensor_id` e `factory.id`.

## Alternativas Consideradas

1. **PostgreSQL com JSONB**: Integridade relacional, suporte ACID e reuso da stack já existente no projeto. JOINs entre as tabelas de Fábricas, Equipamentos e Sensores elevam a latência de resolução das chaves de telemetria recebidas, inviabilizando o uso em tempo real.
2. **MongoDB com Referências Externas (Normalized Approach)**: Evita duplicação dos dados da fábrica. Exige múltiplas queries ou agregações via `$lookup` para reunir equipamento e sensores, encarecendo as consultas frequentes do pipeline de streaming.

## Consequências

### Positivas
- Flexibilidade de schema sem necessidade de migrações estruturais (sem `ALTER TABLE`).
- Resolução do contexto completo do equipamento e de todos os seus sensores em uma única leitura, essencial para o enriquecimento em streaming.
- Modelagem natural para hierarquias One-to-Few em NoSQL orientado a documentos.

### Negativas
- Duplicação dos dados geográficos da fábrica em cada documento de equipamento, exigindo reescritas em massa caso esses dados mudem (evento extremamente infrequente no domínio).
- Limite físico de 16 MB por documento BSON seria atingido em cenário hipotético de equipamentos com milhares de sensores associados.

## Referências
- MongoDB Documentation: Data Modeling Introduction (Embedded vs. References).
- Padrões de modelagem One-to-Few para Data Lakehouse Medallion com dimensões desnormalizadas.

---

# ADR-002: Apache Kafka no Modo KRaft como Plataforma de Streaming

## Status
Aceito

## Contexto
A plataforma precisa de um broker de mensagens capaz de receber eventos contínuos de sensores IoT distribuídos por múltiplas fábricas. Os requisitos são: alta throughput, ordenação garantida por equipamento, retenção configurável de eventos e suporte a múltiplos consumidores independentes.

Adicionalmente, a arquitetura deve minimizar a complexidade operacional para o ambiente de desenvolvimento sem sacrificar a paridade conceitual com ambientes de produção.

## Decisão
Utilizar o **Apache Kafka 7.5.3** no modo **KRaft** (sem dependência do Apache ZooKeeper). O modo KRaft elimina a necessidade de gerenciar um ensemble ZooKeeper separado, consolidando os papéis de broker e controller em um único processo. A configuração adota 3 partições por tópico, fator de replicação 1 (desenvolvimento), retenção de 168 horas e limite de 1 GB por tópico.

## Alternativas Consideradas

1. **Apache Kafka com ZooKeeper**: Modelo de implantação tradicional com alta maturidade e documentação extensa. Exige um ensemble ZooKeeper adicional, aumentando a complexidade operacional e o número de containers no ambiente de desenvolvimento.
2. **RabbitMQ**: Broker de mensagens leve e de fácil operação, adequado para filas de trabalho. Não foi projetado para retenção de eventos, replay de mensagens ou particionamento para processamento paralelo, requisitos fundamentais para pipelines de dados IoT.
3. **Apache Pulsar**: Arquitetura moderna com separação entre computação e armazenamento (Apache BookKeeper). Menor adoção de mercado e ecossistema de ferramentas menos maduro que o Kafka para integrações de Data Lakehouse.
4. **Serviços Gerenciados (AWS MSK, GCP Pub/Sub)**: Eliminam a operação do broker. Criam dependência de provedor de nuvem e inviabilizam o ambiente de desenvolvimento local reproduzível.

## Consequências

### Positivas
- Ordenação garantida por `equipment_id` dentro de cada partição.
- Replay de eventos disponível dentro da janela de retenção configurada (7 dias).
- Modo KRaft elimina a dependência do ZooKeeper, simplificando o `docker-compose` de desenvolvimento.
- Suporte nativo a múltiplos grupos de consumidores independentes para os diferentes estágios do pipeline.

### Negativas
- Overhead operacional significativo em comparação com brokers mais simples para volumes menores.
- Fator de replicação 1 não oferece tolerância a falha; requer ajuste obrigatório antes de qualquer implantação em produção.
- Auto-criação de tópicos habilitada facilita o desenvolvimento mas pode gerar tópicos fantasma em produção.

## Referências
- Apache Kafka KRaft Architecture Documentation.
- Confluent Platform 7.5 Release Notes.

---

# ADR-003: Arquitetura Medallion com MinIO como Data Lake

## Status
Aceito

## Contexto
O volume de eventos IoT gerados continuamente requer uma estratégia de armazenamento escalável que separe dados brutos de dados processados e curados. É necessário garantir rastreabilidade completa desde o evento original até os agregados analíticos, permitindo reprocessamento a qualquer momento.

O ambiente de desenvolvimento deve reproduzir fielmente a interface de armazenamento usada em produção na nuvem (AWS S3).

## Decisão
Adotar a **Arquitetura Medallion** com três camadas: Bronze (dados brutos, imutáveis), Silver (dados limpos, enriquecidos e deduplicados) e Gold (agregações analíticas para consumo por BI e APIs). O armazenamento é provisionado via **MinIO**, um sistema de object storage compatível com a API do Amazon S3, acessível pelo protocolo S3A.

Os buckets são criados e gerenciados via **Terraform** com o provider AWS configurado para apontar ao endpoint do MinIO local. A camada Bronze persiste eventos em formato **JSON Lines (.ndjson)** com particionamento Hive-style: `factory_id=X/measurement_type=Y/dt=Z/batch_*.ndjson`.

## Alternativas Consideradas

1. **Arquitetura Lambda**: Separação entre batch layer e speed layer. Duplica a lógica de processamento entre os caminhos batch e streaming, aumentando o custo de manutenção.
2. **Arquitetura Kappa**: Processamento unificado via streaming para todas as camadas. Eleva a complexidade do pipeline de streaming ao exigir que ele também produza resultados históricos e agregados.
3. **Modelo de Zonas (Raw/Trusted/Refined)**: Nomenclatura alternativa com semântica equivalente. Menos padronizado no ecossistema de ferramentas modernas (Delta Lake, Apache Iceberg, Databricks) que adotam o modelo Medallion.
4. **HDFS como armazenamento**: Tecnologia madura para Hadoop. Não oferece compatibilidade nativa com S3A, dificultando a migração para nuvem e a integração com ferramentas modernas do ecossistema Spark.

## Consequências

### Positivas
- Rastreabilidade completa: dados brutos na camada Bronze nunca são sobrescritos, permitindo reprocessamento integral a qualquer momento.
- Compatibilidade com S3A garante paridade entre ambiente de desenvolvimento (MinIO) e produção (AWS S3) sem alteração de código.
- Particionamento Hive-style permite pruning eficiente nas consultas Spark por fábrica, tipo de medição e data.
- Buckets gerenciados por Terraform garantem reprodutibilidade do ambiente.

### Negativas
- Formato JSON Lines na Bronze não é otimizado para leitura analítica (sem compressão colunar). É necessário o processamento para Silver/Gold para consultas eficientes.
- Duas instâncias de `docker-compose` (raiz e `infra/docker`) criam ambiguidade na sequência de inicialização dos serviços.
- Retenção indefinida na Bronze requer política de ciclo de vida para controle de custos em produção.

## Referências
- Databricks Medallion Architecture Documentation.
- MinIO S3-Compatible API Reference.
- Terraform AWS Provider Documentation.

---

# ADR-004: Apache Spark com Delta Lake para Processamento em Lote

## Status
Aceito

## Contexto
O pipeline de ETL precisa processar grandes volumes de eventos armazenados na camada Bronze, aplicar transformações de qualidade de dados, enriquecer com metadados do MongoDB e persistir os resultados nas camadas Silver e Gold do Data Lake.

As operações de escrita no Data Lake precisam de garantias ACID para suportar reprocessamento parcial sem corrupção de dados.

## Decisão
Utilizar **Apache Spark 3.5.3** em modo Standalone (Master + 1 Worker) com **Delta Lake 3.2.0** como formato de tabela transacional. O processamento segue o padrão Template Method via classe abstrata `AbstractETL`, centralizando lógica de leitura S3A, validação de qualidade e escrita com commit atômico.

A qualidade dos dados é verificada pelo **PyDeequ 2.0.9** (wrapper Python para AWS Deequ) antes da persistência em cada camada. O ambiente Spark utiliza **Python 3.11** sobre **OpenJDK 17 JRE**.

## Alternativas Consideradas

1. **Apache Flink**: Suporte nativo a processamento de streams com baixa latência e semântica exatamente-uma-vez. Curva de aprendizado mais elevada e ecossistema Python menos maduro que o PySpark para workloads batch.
2. **Dask / Ray**: Frameworks Python-nativos com menor overhead de JVM. Sem suporte nativo a Delta Lake e com ecossistema de conectores S3A menos consolidado que o Spark.
3. **Apache Iceberg como formato de tabela**: Alternativa moderna ao Delta Lake com suporte a evolução de schema e time-travel. Menor integração nativa com o ecossistema Databricks e documentação menos extensa para o caso de uso de Data Lakehouse.
4. **Apache Hudi**: Suporte a upserts incrementais e Merge-on-Read. Complexidade operacional maior para o estágio atual do projeto e menor adoção em pipelines batch simples.

## Consequências

### Positivas
- Delta Lake fornece transações ACID, permitindo reprocessamento seguro sem janelas de indisponibilidade.
- Template Method (`AbstractETL`) centraliza lógica transversal (leitura S3A, qualidade, escrita) e permite extensão sem duplicação.
- PyDeequ integra verificações de qualidade diretamente no job Spark, antes do commit de escrita.
- Spark Standalone é suficiente para o volume atual e permite escalonamento horizontal adicionando workers sem reconfigurar o master.

### Negativas
- Modo Standalone não oferece tolerância a falha do master; requer YARN ou Kubernetes para produção de alta disponibilidade.
- JVM sob Python adiciona overhead de inicialização (cold start) elevado para jobs de pequeno volume.
- Worker configurado com 2 GB de memória e 2 núcleos limita o paralelismo em máquinas de desenvolvimento com recursos restritos.

## Referências
- Delta Lake Documentation: ACID Transactions on Apache Spark.
- PyDeequ Documentation: Data Quality Verification with Apache Spark.
- Apache Spark Standalone Mode Cluster Overview.

---

# ADR-005: Confluent Schema Registry com Validação Dual

## Status
Aceito

## Contexto
Eventos IoT publicados no Kafka precisam seguir um schema bem definido para que o pipeline downstream possa processar e validar os dados com confiança. O consumidor da camada Bronze deve rejeitar eventos malformados e isolá-los sem interromper o processamento do fluxo principal.

O ambiente de desenvolvimento precisa funcionar mesmo quando o Schema Registry não está disponível (inicialização parcial dos serviços, testes unitários).

## Decisão
Utilizar o **Confluent Schema Registry 7.5.3** como repositório central de schemas, com formato JSON Schema. O consumidor implementa uma estratégia de **validação dual**: tenta obter o schema do Registry remoto e, em caso de falha de conectividade, aplica um schema local embutido no código (`local_schema.py`) como fallback.

Eventos que falham na validação de schema são direcionados para uma **Dead Letter Queue (DLQ)** persistida em arquivos `.ndjson` isolados dentro do bucket Bronze, no prefixo `bronze/_dead_letter/dt=YYYY-MM-DD/`.

## Alternativas Consideradas

1. **Validação apenas via Registry remoto**: Elimina a duplicidade de schemas no código. Torna o consumidor dependente da disponibilidade do Registry; qualquer falha de conectividade paralisa o processamento.
2. **Sem validação de schema (schema-on-read)**: Máxima tolerância a falhas na ingestão. Erros de schema só são descobertos nas camadas Silver e Gold, aumentando o custo de correção e o risco de dados corrompidos propagarem.
3. **Apache Avro como formato de schema**: Serialização binária compacta com suporte nativo a evolução de schema. Exige serialização/desserialização específica no produtor e consumidor, adicionando complexidade ao simulador IoT e ao consumidor.

## Consequências

### Positivas
- Resiliência operacional: o consumidor continua validando eventos mesmo sem conectividade com o Registry.
- Isolamento de eventos inválidos na DLQ permite auditoria e reprocessamento posterior sem perda de dados.
- Schema centralizado no Registry serve como contrato entre produtor e consumidor, facilitando evolução controlada.

### Negativas
- Dois schemas (Registry e local) precisam ser mantidos em sincronia manualmente; divergência silenciosa é um risco real.
- A estratégia atual não suporta evolução de schema de forma automática; alterações incompatíveis exigem atualização coordenada do produtor, Registry e schema local.
- DLQ em arquivos locais não possui mecanismo de alerta ou reprocessamento automático implementado.

## Referências
- Confluent Schema Registry Documentation: Schema Formats.
- Dead Letter Queue Pattern em pipelines de dados.

---

# ADR-006: Loki, Promtail e Grafana para Observabilidade

## Status
Aceito

## Contexto
O pipeline de streaming e os jobs Spark geram logs de operação que precisam ser agregados, pesquisáveis e visualizáveis de forma centralizada. A solução deve ser leve o suficiente para rodar no ambiente de desenvolvimento junto com os demais serviços do `docker-compose`.

## Decisão
Adotar a pilha **Grafana Loki 2.9.0** como backend de agregação de logs, **Promtail 2.9.0** como agente coletor e **Grafana 10.0.0** como plataforma de visualização e alertas. Os logs são emitidos em formato **JSON Lines estruturado** pelos componentes Python, facilitando a filtragem por campos no Loki. O backend de armazenamento do Loki utiliza `boltdb-shipper` com sistema de arquivos local. A retenção de logs é configurada para 168 horas.

## Alternativas Consideradas

1. **Elasticsearch + Kibana (ELK Stack)**: Plataforma madura com capacidades de busca full-text avançada. Consumo de memória do Elasticsearch (mínimo 2 GB por instância) é incompatível com o ambiente de desenvolvimento com recursos limitados.
2. **Splunk**: Solução enterprise com recursos avançados de correlação e alertas. Custo de licenciamento e complexidade de configuração estão fora do escopo de um ambiente de desenvolvimento acadêmico/laboratorial.
3. **CloudWatch / Datadog**: Serviços gerenciados com excelente integração com infraestrutura de nuvem. Dependência de provedor externo e custo variável por volume de logs são inadequados para ambiente local.

## Consequências

### Positivas
- Loki é significativamente mais leve que o Elasticsearch, viável dentro do `docker-compose` de desenvolvimento.
- Logs estruturados em JSON permitem filtragem por campos específicos (ex: `level`, `consumer_group`, `factory_id`) diretamente no Grafana.
- Stack Grafana unificada permite evolução futura para métricas (Prometheus) e traces (Tempo) sem troca de plataforma de visualização.

### Negativas
- Loki não oferece busca full-text sobre o conteúdo dos logs; apenas filtragem por labels e expressões LogQL. Consultas complexas são menos intuitivas que o Kibana Query Language.
- Backend `boltdb-shipper` não é recomendado para produção de alto volume; requer migração para S3 ou GCS como backend de armazenamento.
- Métricas de runtime (Prometheus) não estão implementadas; a observabilidade atual cobre apenas logs, deixando lacuna de visibilidade em throughput, latência e uso de recursos.

## Referências
- Grafana Loki Documentation: Architecture and Deployment Modes.
- Promtail Configuration Reference.

---

# ADR-007: PostgreSQL para Dados Operacionais Legados

## Status
Aceito

## Contexto
O projeto integra dados de um sistema ERP legado que mantém registros operacionais de fábricas, equipamentos, sensores e leituras históricas em um banco de dados relacional. Esses dados precisam ser acessíveis para o pipeline de ETL e servem como fonte de dados históricos para as camadas Silver e Gold.

## Decisão
Utilizar o **PostgreSQL 16.13** com schema normalizado na **3ª Forma Normal (3FN)**, refletindo a modelagem do sistema ERP de origem. O schema inclui triggers automáticos para atualização de `updated_at` em todas as tabelas mutáveis, índices compostos em `(sensor_id, timestamp)` para consultas temporais e uso de `TIMESTAMPTZ` para todos os campos de data e hora.

## Alternativas Consideradas

1. **Schema em Star Schema (Data Warehouse)**: Otimizado para consultas analíticas com dimensões desnormalizadas. Quebraria a fidelidade ao modelo de origem do ERP legado, exigindo transformação antes da ingestão e dificultando a rastreabilidade entre o dado original e o dado no Data Lake.
2. **PostgreSQL com JSONB para dados de sensores**: Flexibilidade de schema para leituras heterogêneas. Perde os benefícios de índices B-tree eficientes em consultas temporais de alta seletividade e dificulta validação de integridade referencial.
3. **MySQL / MariaDB**: Bancos relacionais alternativos com menor overhead operacional. Ausência de suporte nativo a `TIMESTAMPTZ`, funções de janela avançadas e extensibilidade via extensões (ex: TimescaleDB) limitam a evolução futura da plataforma.

## Consequências

### Positivas
- Integridade referencial via chaves estrangeiras garante consistência dos dados operacionais.
- Índices compostos em `(sensor_id, timestamp)` habilitam consultas temporais eficientes para o ETL da camada Bronze para Silver.
- `TIMESTAMPTZ` elimina ambiguidades de fuso horário em dados distribuídos geograficamente entre fábricas.
- Triggers de auditoria (`updated_at`) oferecem rastreabilidade de modificações sem lógica adicional na aplicação.

### Negativas
- Schema 3FN exige JOINs múltiplos para consultas analíticas, tornando o PostgreSQL inadequado como fonte direta para dashboards; os dados devem ser promovidos para a camada Gold antes do consumo analítico.
- A camada de ETL do PostgreSQL para Bronze ainda não está implementada para todos os tipos de dados históricos (pendência identificada no backlog).

## Referências
- PostgreSQL 16 Documentation: Indexes, Triggers, and Timestamp Types.
- Kimball Data Warehouse Toolkit: Star Schema vs. Normalized Models.

---

# ADR-008: Terraform para Provisionamento de Infraestrutura

## Status
Aceito

## Contexto
Os buckets do Data Lake (Bronze, Silver, Gold) no MinIO precisam ser criados de forma reproduzível em qualquer ambiente (desenvolvimento local, CI, staging, produção). A criação manual via console ou scripts imperativos não garante idempotência nem rastreabilidade das configurações de infraestrutura.

## Decisão
Utilizar o **Terraform** com o provider AWS configurado para apontar ao endpoint local do MinIO (`http://localhost:9000`). Os três buckets do Medallion (`bronze`, `silver`, `gold`) são declarados como recursos `aws_s3_bucket`. As credenciais de acesso são injetadas via variáveis de ambiente a partir do arquivo `.env`, evitando segredos hardcoded nos arquivos Terraform.

## Alternativas Consideradas

1. **Scripts boto3 (Python)**: Criação programática dos buckets com a SDK AWS para Python. Exige lógica explícita de idempotência (verificar existência antes de criar) e não oferece rastreamento de estado (drift detection).
2. **Criação manual via console MinIO**: Adequada para configurações pontuais. Não é reproduzível, não está versionada e não é adequada para automação de CI/CD.
3. **Pulumi**: IaC com suporte a linguagens de programação (Python, TypeScript). Ecossistema menor que o Terraform e menos familiaridade na equipe para o escopo atual do projeto.

## Consequências

### Positivas
- Infraestrutura declarativa e versionada junto ao código, garantindo que qualquer membro da equipe possa recriar o ambiente de desenvolvimento com um único comando.
- Idempotência nativa do Terraform: executar `terraform apply` múltiplas vezes não cria recursos duplicados.
- Configuração preparada para migração de MinIO para AWS S3 real alterando apenas o endpoint e as credenciais no provider.

### Negativas
- Overhead de aprendizado do Terraform para contribuidores familiarizados apenas com Python.
- Estado local do Terraform (`terraform.tfstate`) não é compartilhado entre membros da equipe; requer configuração de backend remoto (S3, Terraform Cloud) antes da colaboração em time.
- A configuração atual não define políticas de bucket, versionamento ou criptografia em repouso, requisitos obrigatórios antes de qualquer implantação em produção.

## Referências
- Terraform AWS Provider Documentation: `aws_s3_bucket` resource.
- MinIO Terraform Provider Compatibility Notes.

---

# ADR-009: Padrão de Processamento Silver com Classe Base e Delta Lake

## Status
Aceito

## Contexto
A camada Silver precisa processar dados de três origens heterogêneas (PostgreSQL via Parquet, MongoDB via Parquet e Kafka via NDJSON) aplicando limpeza, tipagem forte e deduplicação em cada tabela. Sem um padrão centralizado, cada job ETL repetiria a lógica de leitura do bucket Bronze, deduplicação e escrita Delta Lake, criando duplicação e inconsistência de comportamento entre os 10 jobs.

## Decisão
Introduzir a classe abstrata `SilverETL` (em `src/processamento/silver/base_silver_etl.py`) que estende `AbstractETL`. Ela encapsula três responsabilidades transversais: (1) resolução dinâmica do bucket de origem Bronze segundo o ambiente (`bronze` em prd, `bronze-hmg` em hmg), (2) métodos de leitura tipados `_read_bronze_parquet()` e `_read_bronze_ndjson()` que constroem o path S3A completo, e (3) `_deduplicate(keys)` com log de registros removidos.

Cada um dos 10 ETLs Silver herda de `SilverETL` e implementa apenas `extract()`, `transform()`, `load()` e `unit_tests()`, seguindo o Template Method definido em `AbstractETL.run()`.

A escrita Silver usa exclusivamente `upsert_delta_table()` com `previous_delete=False`, garantindo que histórico de partições anteriores seja preservado e apenas atualizações e inserções sejam aplicadas via Delta Merge.

## Alternativas Consideradas

1. **Jobs Silver independentes sem classe base**: Cada job autossuficiente com sua própria lógica de leitura e escrita. Elimina a dependência de herança mas duplica ~40 linhas de código de infraestrutura em cada um dos 10 jobs, dificultando mudanças transversais (ex: alterar o nome do bucket Bronze).
2. **Configuração por injeção de dependência (Composition over Inheritance)**: Passar um objeto `BronzeReader` para cada job em vez de herdar. Mais flexível para testes unitários. Aumenta a verbosidade da instanciação e exige documentação adicional do contrato de interface, overhead não justificado para o volume atual de jobs.
3. **Delta Live Tables (DLT)**: Framework Databricks nativo para pipelines Medallion com checagem de qualidade integrada. Requer Databricks Runtime, incompatível com o ambiente local Spark Standalone sobre Docker.

## Consequências

### Positivas
- Mudanças no padrão de leitura Bronze ou na lógica de deduplicação propagam para todos os 10 jobs Silver sem alteração individual.
- O histórico de cada tabela Silver é preservado por `previous_delete=False`; reprocessamentos da mesma `_execution_date` apenas atualizam registros existentes via Merge.
- A validação PyDeequ herdada de `AbstractETL` é executada automaticamente antes de cada escrita Silver, sem configuração adicional por job.

### Negativas
- Herança profunda (`AbstractETL` → `SilverETL` → `FabricasETL`) dificulta o rastreamento do fluxo de execução sem leitura da classe base.
- `_deduplicate()` opera sobre `self.df` com efeito colateral (mutação de estado), o que dificulta testes unitários isolados de `transform()`.
- O comportamento de abort silencioso de `AbstractETL.run()` quando o DataFrame está vazio após `transform()` (log WARNING + retorno sem exceção) pode mascarar falhas de extração; requer monitoramento ativo dos logs loguru.

## Referências
- Delta Lake Merge Documentation: `whenMatchedUpdateAll` / `whenNotMatchedInsertAll`.
- Template Method Pattern — Gang of Four Design Patterns.
- PyDeequ: Data Quality Verification on Apache Spark.
