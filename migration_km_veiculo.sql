-- ============================================================
-- MIGRATION: Adiciona veiculo_id nas tabelas de KM
-- HubTécnico CliqueDf · IaTechHub · 2026
-- Rodar uma única vez no servidor:
--   sqlite3 /opt/automacoes/cliquedf/tecnico/hub_tecnico.db < migration_km_veiculo.sql
-- ============================================================

-- 1. Adiciona coluna veiculo_id em ht_km_os (se ainda não existir)
ALTER TABLE ht_km_os ADD COLUMN veiculo_id INTEGER REFERENCES ht_veiculos(id);

-- 2. Adiciona coluna updated_at em ht_km_os (para controle de edição)
ALTER TABLE ht_km_os ADD COLUMN updated_at TEXT;

-- 3. Índice para busca rápida por veículo
CREATE INDEX IF NOT EXISTS idx_km_os_veiculo ON ht_km_os(veiculo_id);

-- 4. Índice para busca rápida por técnico + veículo
CREATE INDEX IF NOT EXISTS idx_km_os_tecnico_veiculo ON ht_km_os(tecnico_id, veiculo_id);

-- 5. View auxiliar: último KM por veículo (facilita consultas de frota)
CREATE VIEW IF NOT EXISTS vw_ultimo_km_veiculo AS
SELECT
    veiculo_id,
    MAX(km) AS ultimo_km,
    tecnico_id AS ultimo_tecnico_id
FROM (
    SELECT veiculo_id, tecnico_id, km_chegada AS km
    FROM ht_km_os WHERE km_chegada IS NOT NULL AND veiculo_id IS NOT NULL
    UNION ALL
    SELECT veiculo_id, tecnico_id, km_saida AS km
    FROM ht_km_os WHERE km_saida IS NOT NULL AND veiculo_id IS NOT NULL
)
GROUP BY veiculo_id;

-- 6. View auxiliar: KM rodado por veículo por período (para relatório Frota)
CREATE VIEW IF NOT EXISTS vw_km_por_veiculo AS
SELECT
    k.veiculo_id,
    v.placa,
    v.modelo,
    v.marca,
    DATE(k.created_at) AS data,
    k.tecnico_id,
    u.nome AS tecnico_nome,
    SUM(k.km_deslocamento) AS km_total_dia,
    COUNT(k.os_id) AS os_realizadas
FROM ht_km_os k
JOIN ht_veiculos v ON v.id = k.veiculo_id
JOIN ht_usuarios u ON u.id = k.tecnico_id
WHERE k.km_deslocamento IS NOT NULL
GROUP BY k.veiculo_id, DATE(k.created_at), k.tecnico_id;

-- ============================================================
-- Verificar resultado:
-- SELECT * FROM vw_ultimo_km_veiculo;
-- SELECT * FROM vw_km_por_veiculo ORDER BY data DESC LIMIT 20;
-- ============================================================
