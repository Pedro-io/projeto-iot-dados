# Observabilidade - Logs com Grafana + Loki + Promtail

Stack de observabilidade baseada em logs dos containers Docker.

---

## Arquitetura

```
Containers Docker → Promtail → Loki → Grafana
   (stdout/stderr)   (coleta)  (armazena) (visualiza)
```

| Componente | Papel | Porta |
|---|---|---|
| Promtail | Coleta logs dos containers e envia ao Loki | 9080 |
| Loki | Armazena e indexa os logs (por labels) | 3100 |
| Grafana | Interface de dashboards, Explore e alertas | 3001 |

---

## Acessando o Grafana

```bash
# Subir a stack (inclui Loki, Promtail e Grafana)
docker compose -f infra/docker/docker-compose.yml up -d
```

Acesse: `http://localhost:3001`
- Usuário: `admin`
- Senha: `admin`

---

## Consultando logs (LogQL)

No Grafana, vá em **Explore** → selecione datasource **Loki**.

**Ver todos os logs dos containers:**
```logql
{job="docker"}
```

**Filtrar por texto:**
```logql
{job="docker"} |= "ERROR"
```

**Filtrar por container:**
```logql
{job="docker", container="/kafka"}
```

**Ver apenas logs do consumer Bronze:**
```logql
{job="docker"} |= "bronze"
```

---

## Estrutura de arquivos

```
observability/
├── grafana/
│   └── provisioning/
│       └── datasources/
│           └── loki.yml       # Datasource Loki provisionado automaticamente
└── promtail-config.yml        # Coleta logs de /var/lib/docker/containers/
```

O datasource Loki é provisionado automaticamente via arquivo - não é necessário configurá-lo manualmente na UI.

---

## Pendências

- [ ] Adicionar Prometheus como datasource para métricas (E3)
- [ ] Instrumentar consumers Python com `prometheus-client`
- [ ] Criar dashboards provisionados em `provisioning/dashboards/`
- [ ] Configurar alertas no Grafana
