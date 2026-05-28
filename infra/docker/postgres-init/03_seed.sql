-- ============================================================
-- ERP LEGADO — Dados de carga inicial (lookup + mestres fixos)
-- ============================================================

INSERT INTO erp_legado.tipos_equipamento (nome, descricao) VALUES
    ('compressor', 'Compressor industrial'),
    ('pump',       'Bomba hidráulica'),
    ('motor',      'Motor elétrico'),
    ('conveyor',   'Esteira transportadora'),
    ('turbine',    'Turbina industrial');

INSERT INTO erp_legado.tipos_medicao
    (nome, unidade, valor_minimo, valor_maximo, faixa_normal_min, faixa_normal_max)
VALUES
    ('temperature', 'celsius',  20,  100, 30,  70),
    ('humidity',    'percent',   0,  100, 40,  60),
    ('pressure',    'bar',        0,   20,  5,  15),
    ('vibration',   'mm/s',       0,   50,  0,  10),
    ('current',     'ampere',     0,  100, 10,  50);

INSERT INTO erp_legado.status_qualidade (nome) VALUES
    ('good'),
    ('warning'),
    ('bad');

INSERT INTO erp_legado.fabricas (id, nome, latitude, longitude) VALUES
    ('FAB-SP-01', 'Fábrica São Paulo',       -23.550500, -46.633300),
    ('FAB-RJ-01', 'Fábrica Rio de Janeiro',  -22.906800, -43.172900),
    ('FAB-MG-01', 'Fábrica Belo Horizonte',  -19.916700, -43.934500);
