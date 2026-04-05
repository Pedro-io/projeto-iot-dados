# Modelagem de Dados NoSQL (MongoDB)

Este documento detalha o modelo de dados NoSQL projetado para o **Cadastro de Equipamentos**. O **MongoDB** foi adotado como solução devido à natureza semiestruturada e evolutiva dos dados.

## Estrutura do Documento: Equipamento

Cada documento na coleção `equipments` no MongoDB representa uma entidade única de equipamento, encapsulando também os dados de sua localização (fábrica) e a lista de sensores associados a ele.

```json
{
  "_id": "EQ-1234",
  "name": "Compressor Industrial A1",
  "type": "compressor",
  "factory": {
    "id": "FAB-SP-01",
    "name": "Fábrica São Paulo",
    "location": {"lat": -23.5505, "lng": -46.6333}
  },
  "sensors": [
    {"id": "SENS-001-TEMP", "type": "temperature", "range": {"min": 0, "max": 150}},
    {"id": "SENS-002-VIBR", "type": "vibration", "range": {"min": 0, "max": 100}}
  ],
  "maintenance_schedule": "monthly",
  "installed_at": "2023-06-15T00:00:00Z",
  "status": "active"
}
```

## Dicionário de Dados

| Campo | Tipo no BSON | Descrição | Obrigatório |
|---|---|---|---|
| `_id` | String | Identificador único do equipamento (Ex: `EQ-1234`). | Sim |
| `name` | String | Nome descritivo do equipamento. | Sim |
| `type` | String | Categoria do equipamento (Ex: `compressor`, `esteira`). | Sim |
| `factory` | Object | Objeto embutido (*embedded*) contendo dados da fábrica onde está instalado. | Sim |
| `factory.id` | String | Identificador da fábrica. Usado para agregar/filtrar equipamentos por localidade. | Sim |
| `factory.name` | String | Nome da fábrica. | Sim |
| `factory.location`| Object | Coordenadas geoespaciais (latitude e longitude) para plotagem em dashboards. | Não |
| `sensors` | Array de Objects| Lista de sensores associados (*embedded document pattern*). | Sim |
| `sensors.id`| String | ID único do sensor. Crucial para fazer o *join* lógico com o streaming (Kafka). | Sim |
| `sensors.type`| String | Tipo da métrica aferida (Ex: `temperature`, `vibration`). | Sim |
| `sensors.range` | Object | Faixa de operação normal do sensor (usado pra cálculos de anomalia depois). | Não |
| `maintenance_schedule`| String| Plano de manutenção do equipamento (Ex: `monthly`, `weekly`, `daily`). | Não |
| `installed_at` | DateTime | Data em que o equipamento foi fisicamente instalado (Formato ISO). | Não |
| `status` | String | Status operacional atual (`active`, `inactive`, `maintenance`, `broken`). | Sim |

## Padrões de Modelagem Utilizados

### 1. Padrão de Documento Embutido (*Embedded Document / One-to-Few*)
Optou-se por **embutir** (`embedding`) a lista de `sensors` no documento do equipamento (*One-to-Few*).
**Justificativa**: Sensores são dependentes de um equipamento. O equipamento geralmente é consultado junto com seus sensores para configurar limites ou validá-los no processamento. Objetos separados exigiriam JOINs lógicos frequentes. O limite de 16MB do MongoDB não será atingido, visto que os equipamentos possuem apenas dezenas de sensores.

### 2. Desnormalização (Atributos de Fábrica)
Os dados de referência da `factory` foram embutidos.
**Justificativa**: Evita a busca cruzada por dados de fábricas a cada consulta de equipamento, acelerando as leituras (especialmente no enriquecimento na camada **Silver**).

## Estratégia de Índices

Para garantir melhor performance de pesquisa, foram propostos os seguintes índices:

1. **Índice composto por Fábrica e Status**:
   `db.equipments.createIndex({ "factory.id": 1, "status": 1 })`
   *Motivação*: Otimiza listagens de equipamentos ativos por fábrica.
2. **Índice no ID do Sensor (Array)**:
   `db.equipments.createIndex({ "sensors.id": 1 })`
   *Motivação*: Agiliza a consulta de qual equipamento gerou uma telemetria recebida via Kafka (resolução no array `sensors` via Multikey Index).
