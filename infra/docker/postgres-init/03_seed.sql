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

INSERT INTO erp_legado.equipamentos (id, nome, tipo_equipamento_id, fabrica_id, data_instalacao, status) VALUES
    ('EQ-SP-001', 'Compressor A1',        1, 'FAB-SP-01', '2021-03-15', 'ativo'),
    ('EQ-SP-002', 'Bomba Hidráulica B2',  2, 'FAB-SP-01', '2020-07-22', 'ativo'),
    ('EQ-SP-003', 'Motor Elétrico C3',    3, 'FAB-SP-01', '2019-11-10', 'manutencao'),
    ('EQ-RJ-001', 'Esteira Principal D1', 4, 'FAB-RJ-01', '2022-01-05', 'ativo'),
    ('EQ-RJ-002', 'Turbina Industrial E2',5, 'FAB-RJ-01', '2020-09-30', 'ativo'),
    ('EQ-MG-001', 'Compressor MG-1',      1, 'FAB-MG-01', '2021-06-18', 'ativo'),
    ('EQ-MG-002', 'Motor Tração F4',      3, 'FAB-MG-01', '2023-02-14', 'inativo');

INSERT INTO erp_legado.sensores (id, equipamento_id, tipo_medicao_id, data_instalacao, status) VALUES
    ('SEN-SP-001', 'EQ-SP-001', 1, '2021-03-15', 'ativo'),   -- temperatura
    ('SEN-SP-002', 'EQ-SP-001', 3, '2021-03-15', 'ativo'),   -- pressão
    ('SEN-SP-003', 'EQ-SP-002', 4, '2020-07-22', 'ativo'),   -- vibração
    ('SEN-SP-004', 'EQ-SP-002', 5, '2020-07-22', 'ativo'),   -- corrente
    ('SEN-SP-005', 'EQ-SP-003', 1, '2019-11-10', 'manutencao'),
    ('SEN-RJ-001', 'EQ-RJ-001', 2, '2022-01-05', 'ativo'),   -- umidade
    ('SEN-RJ-002', 'EQ-RJ-001', 4, '2022-01-05', 'ativo'),   -- vibração
    ('SEN-RJ-003', 'EQ-RJ-002', 1, '2020-09-30', 'ativo'),   -- temperatura
    ('SEN-MG-001', 'EQ-MG-001', 3, '2021-06-18', 'ativo'),   -- pressão
    ('SEN-MG-002', 'EQ-MG-002', 5, '2023-02-14', 'inativo'); -- corrente

INSERT INTO erp_legado.leituras (id, sensor_id, valor, status_qualidade_id, is_anomalia, timestamp_leitura) VALUES
    ('a1b2c3d4-0001-0001-0001-000000000001', 'SEN-SP-001', 45.30, 1, FALSE, NOW() - INTERVAL '10 minutes'),
    ('a1b2c3d4-0001-0001-0001-000000000002', 'SEN-SP-001', 72.80, 2, FALSE, NOW() - INTERVAL '8 minutes'),
    ('a1b2c3d4-0001-0001-0001-000000000003', 'SEN-SP-001', 95.10, 3, TRUE,  NOW() - INTERVAL '6 minutes'),
    ('a1b2c3d4-0001-0001-0001-000000000004', 'SEN-SP-002', 8.50,  1, FALSE, NOW() - INTERVAL '10 minutes'),
    ('a1b2c3d4-0001-0001-0001-000000000005', 'SEN-SP-002', 14.20, 2, FALSE, NOW() - INTERVAL '5 minutes'),
    ('a1b2c3d4-0001-0001-0001-000000000006', 'SEN-SP-003', 3.70,  1, FALSE, NOW() - INTERVAL '9 minutes'),
    ('a1b2c3d4-0001-0001-0001-000000000007', 'SEN-SP-003', 12.40, 3, TRUE,  NOW() - INTERVAL '4 minutes'),
    ('a1b2c3d4-0001-0001-0001-000000000008', 'SEN-SP-004', 35.00, 1, FALSE, NOW() - INTERVAL '7 minutes'),
    ('a1b2c3d4-0001-0001-0001-000000000009', 'SEN-RJ-001', 52.30, 1, FALSE, NOW() - INTERVAL '11 minutes'),
    ('a1b2c3d4-0001-0001-0001-000000000010', 'SEN-RJ-002', 5.10,  1, FALSE, NOW() - INTERVAL '3 minutes'),
    ('a1b2c3d4-0001-0001-0001-000000000011', 'SEN-RJ-003', 61.00, 2, FALSE, NOW() - INTERVAL '6 minutes'),
    ('a1b2c3d4-0001-0001-0001-000000000012', 'SEN-MG-001', 6.80,  1, FALSE, NOW() - INTERVAL '2 minutes');

INSERT INTO erp_legado.metadados_leitura (leitura_id, versao_firmware, nivel_bateria, forca_sinal_dbm) VALUES
    ('a1b2c3d4-0001-0001-0001-000000000001', 'v2.1.4', 87, -65),
    ('a1b2c3d4-0001-0001-0001-000000000002', 'v2.1.4', 85, -67),
    ('a1b2c3d4-0001-0001-0001-000000000003', 'v2.1.4', 83, -70),
    ('a1b2c3d4-0001-0001-0001-000000000004', 'v2.0.9', 92, -55),
    ('a1b2c3d4-0001-0001-0001-000000000005', 'v2.0.9', 90, -58),
    ('a1b2c3d4-0001-0001-0001-000000000006', 'v1.8.2', 45, -80),
    ('a1b2c3d4-0001-0001-0001-000000000007', 'v1.8.2', 42, -83),
    ('a1b2c3d4-0001-0001-0001-000000000008', 'v2.1.4', 78, -60),
    ('a1b2c3d4-0001-0001-0001-000000000009', 'v2.2.0', 95, -50),
    ('a1b2c3d4-0001-0001-0001-000000000010', 'v2.2.0', 94, -52),
    ('a1b2c3d4-0001-0001-0001-000000000011', 'v2.1.1', 70, -72),
    ('a1b2c3d4-0001-0001-0001-000000000012', 'v2.0.9', 88, -61);
