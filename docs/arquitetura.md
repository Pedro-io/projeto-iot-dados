# Decisões Arquiteturais 

Este documento centraliza os registros de decisões arquiteturais (*Architecture Decision Records* - ADRs) do projeto Plataforma de Integração de Dados IoT.

---

# ADR-001: Escolha do Banco de Dados e Modelagem (NoSQL) para Cadastro de Equipamentos

## Status
Aceito

## Contexto
É necessário armazenar o cadastro mestre dos equipamentos industriais. Tais equipamentos possuem estrutura hierárquica (fábrica → equipamento → múltiplos sensores) e evolutiva, com sensores variando em quantidade, tipo e limiares operacionais (`range`).

Foi requerida uma solução flexível e de baixa latência em leitura, para que o pipeline de streaming extraia metadados atômica e rapidamente ao processar eventos do Kafka.

## Decisão
Utilizar o **MongoDB** (um banco de dados NoSQL Orientado a Documentos) como banco de referência (Master Data) para o cadastro de equipamentos, utilizando a estratégia de **Documentos Embutidos** (*Embedding Documents*), agregando as características da fábrica e os arranjos de sensores embutidos no escopo do documento-pai do Equipamento, e garantindo indexação secundária como no formato *Multikey Index*.

## Alternativas Consideradas

1. **PostgreSQL com JSONB**:
   - *Prós*: Integridade relacional, suporte ACID e reuso da stack oficial (ERP/Batch legado).
   - *Contras*: JOINs entre tabelas de Fábricas, Equipamentos e Sensores elevam significativamente a latência na resolução das chaves de telemetria recebidas.
2. **MongoDB com Referências Externas (*Normalized Approach*)**:
   - *Prós*: Evita redundância de dados da fábrica (modelagem normalizada).
   - *Contras*: Exige múltiplas *queries* ou agregações (`$lookup`) para unificar o equipamento e seus sensores, encarecendo consultas frequentes.

## Consequências

### Positivas
- **Flexibilidade de Schema**: Permite adicionar novos equipamentos ou sensores sem migrações de esquema (e.g., sem `ALTER TABLE`).
- **Resolução em Query Única (*Single Read*)**: Obtenção ágil do contexto do equipamento e de todos os seus sensores em uma única leitura (crucial para o enriquecimento em Stream).
- **Consultas em listas hierárquicas**: Padrão natural em NoSQL orientado a documentos.

### Negativas
- **Duplicação Geográfica**: Dados da fábrica são replicados nos equipamentos, exigindo reescritas em massa (*bulk overwrite*) caso mudem (extremamente infrequente no domínio).
- **Limite Físico (16MB)**: O limite de tamanho do BSON seria atingido no caso hipotético da associação de milhares de sensores a um único equipamento.

## Referências
- Documentação de Modelagem do MongoDB para *One-to-Few*.
- Exemplários de arquiteturas Data Lakehouse Medallion com dimensões desnormalizadas (Gold layer e Enriquecimento em Stream).
