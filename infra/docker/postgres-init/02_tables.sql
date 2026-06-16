-- ============================================================
-- ERP LEGADO — DDL
-- Modelo normalizado (3NF) para dados de sensores industriais
-- ============================================================

-- ---------------------
-- FUNÇÃO UTILITÁRIA
-- ---------------------

CREATE OR REPLACE FUNCTION erp_legado.fn_set_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;


-- ---------------------
-- TABELAS DE DOMÍNIO
-- ---------------------

CREATE TABLE erp_legado.tipos_equipamento (
    id        SERIAL       PRIMARY KEY,
    nome      VARCHAR(50)  NOT NULL UNIQUE,
    descricao TEXT
);

CREATE TABLE erp_legado.tipos_medicao (
    id               SERIAL         PRIMARY KEY,
    nome             VARCHAR(50)    NOT NULL UNIQUE,
    unidade          VARCHAR(20)    NOT NULL,
    valor_minimo     NUMERIC(10,2)  NOT NULL,
    valor_maximo     NUMERIC(10,2)  NOT NULL,
    faixa_normal_min NUMERIC(10,2)  NOT NULL,
    faixa_normal_max NUMERIC(10,2)  NOT NULL,
    CONSTRAINT chk_medicao_ranges CHECK (
        valor_minimo     <  valor_maximo
        AND faixa_normal_min >= valor_minimo
        AND faixa_normal_max <= valor_maximo
        AND faixa_normal_min <  faixa_normal_max
    )
);

CREATE TABLE erp_legado.status_qualidade (
    id   SERIAL      PRIMARY KEY,
    nome VARCHAR(20) NOT NULL UNIQUE
);


-- ---------------------
-- TABELAS MESTRE
-- ---------------------

CREATE TABLE erp_legado.fabricas (
    id         VARCHAR(20)  PRIMARY KEY,
    nome       VARCHAR(100) NOT NULL,
    latitude   NUMERIC(9,6) NOT NULL,
    longitude  NUMERIC(9,6) NOT NULL,
    created_at TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE TRIGGER trg_fabricas_updated_at
    BEFORE UPDATE ON erp_legado.fabricas
    FOR EACH ROW EXECUTE FUNCTION erp_legado.fn_set_updated_at();


CREATE TABLE erp_legado.equipamentos (
    id                  VARCHAR(20)  PRIMARY KEY,
    nome                VARCHAR(100) NOT NULL,
    tipo_equipamento_id INTEGER      NOT NULL
        REFERENCES erp_legado.tipos_equipamento(id),
    fabrica_id          VARCHAR(20)  NOT NULL
        REFERENCES erp_legado.fabricas(id),
    data_instalacao     DATE,
    status              VARCHAR(20)  NOT NULL DEFAULT 'ativo'
        CONSTRAINT chk_equipamento_status CHECK (status IN ('ativo', 'inativo', 'manutencao')),
    created_at          TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_equipamentos_fabrica_id      ON erp_legado.equipamentos(fabrica_id);
CREATE INDEX idx_equipamentos_tipo_id         ON erp_legado.equipamentos(tipo_equipamento_id);

CREATE TRIGGER trg_equipamentos_updated_at
    BEFORE UPDATE ON erp_legado.equipamentos
    FOR EACH ROW EXECUTE FUNCTION erp_legado.fn_set_updated_at();


CREATE TABLE erp_legado.sensores (
    id              VARCHAR(20)  PRIMARY KEY,
    equipamento_id  VARCHAR(20)  NOT NULL
        REFERENCES erp_legado.equipamentos(id),
    tipo_medicao_id INTEGER      NOT NULL
        REFERENCES erp_legado.tipos_medicao(id),
    data_instalacao DATE,
    status          VARCHAR(20)  NOT NULL DEFAULT 'ativo'
        CONSTRAINT chk_sensor_status CHECK (status IN ('ativo', 'inativo', 'manutencao')),
    created_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_sensores_equipamento_id  ON erp_legado.sensores(equipamento_id);
CREATE INDEX idx_sensores_tipo_medicao_id ON erp_legado.sensores(tipo_medicao_id);

CREATE TRIGGER trg_sensores_updated_at
    BEFORE UPDATE ON erp_legado.sensores
    FOR EACH ROW EXECUTE FUNCTION erp_legado.fn_set_updated_at();


-- ---------------------
-- TABELAS TRANSACIONAIS
-- ---------------------

CREATE TABLE erp_legado.leituras (
    id                  UUID          PRIMARY KEY DEFAULT gen_random_uuid(),
    sensor_id           VARCHAR(20)   NOT NULL
        REFERENCES erp_legado.sensores(id),
    valor               NUMERIC(10,2) NOT NULL,
    status_qualidade_id INTEGER       NOT NULL
        REFERENCES erp_legado.status_qualidade(id),
    is_anomalia         BOOLEAN       NOT NULL DEFAULT FALSE,
    timestamp_leitura   TIMESTAMPTZ   NOT NULL DEFAULT NOW(),
    created_at          TIMESTAMPTZ   NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_leituras_sensor_id         ON erp_legado.leituras(sensor_id);
CREATE INDEX idx_leituras_timestamp         ON erp_legado.leituras(timestamp_leitura DESC);
CREATE INDEX idx_leituras_sensor_timestamp  ON erp_legado.leituras(sensor_id, timestamp_leitura DESC);
CREATE INDEX idx_leituras_anomalias         ON erp_legado.leituras(sensor_id, timestamp_leitura DESC)
    WHERE is_anomalia = TRUE;


CREATE TABLE erp_legado.metadados_leitura (
    leitura_id      UUID      PRIMARY KEY
        REFERENCES erp_legado.leituras(id) ON DELETE CASCADE,
    versao_firmware VARCHAR(20),
    nivel_bateria   SMALLINT  CONSTRAINT chk_bateria  CHECK (nivel_bateria  BETWEEN 0   AND 100),
    forca_sinal_dbm SMALLINT  CONSTRAINT chk_sinal    CHECK (forca_sinal_dbm BETWEEN -120 AND 0)
);
