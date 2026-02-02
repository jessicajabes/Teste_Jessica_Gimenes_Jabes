-- =====================================================
-- IMPORTAÇÃO DE DADOS DOS CSVs usando COPY
-- =====================================================

-- Desabilitar FK constraints temporariamente
ALTER TABLE consolidados_despesas DISABLE TRIGGER ALL;
ALTER TABLE consolidados_despesas_c_deducoes DISABLE TRIGGER ALL;
ALTER TABLE despesas_agregadas DISABLE TRIGGER ALL;
ALTER TABLE despesas_agregadas_c_deducoes DISABLE TRIGGER ALL;

-- =====================================================
-- LIMPAR DADOS ANTIGOS
-- =====================================================

DELETE FROM consolidados_despesas_c_deducoes;
DELETE FROM consolidados_despesas;
DELETE FROM despesas_agregadas_c_deducoes;
DELETE FROM despesas_agregadas;
DELETE FROM operadoras;

-- =====================================================
-- 1. STAGING TABLES
-- =====================================================

DROP TABLE IF EXISTS stg_operadoras CASCADE;
CREATE TEMP TABLE stg_operadoras (
    cnpj TEXT,
    reg_ans TEXT,
    razao_social TEXT,
    modalidade TEXT,
    uf TEXT
);

DROP TABLE IF EXISTS stg_sinistro_sem CASCADE;
CREATE TEMP TABLE stg_sinistro_sem (
    reg_ans TEXT,
    cnpj TEXT,
    razao_social TEXT,
    trimestre TEXT,
    ano TEXT,
    valor_despesas TEXT
);

DROP TABLE IF EXISTS stg_sinistro_com CASCADE;
CREATE TEMP TABLE stg_sinistro_com (
    cnpj TEXT,
    razao_social TEXT,
    trimestre TEXT,
    ano TEXT,
    valor_despesas TEXT,
    reg_ans TEXT,
    conta_contabil TEXT,
    descricao TEXT
);

DROP TABLE IF EXISTS stg_despesas_agregadas CASCADE;
CREATE TEMP TABLE stg_despesas_agregadas (
    razao_social TEXT,
    uf TEXT,
    total_despesas TEXT,
    media_despesas_trimestre TEXT,
    desvio_padrao_despesas TEXT,
    qtd_registros TEXT,
    qtd_trimestres TEXT,
    qtd_anos TEXT
);

DROP TABLE IF EXISTS stg_despesas_agregadas_c_deducoes CASCADE;
CREATE TEMP TABLE stg_despesas_agregadas_c_deducoes (
    razao_social TEXT,
    uf TEXT,
    total_despesas TEXT,
    media_despesas_trimestre TEXT,
    desvio_padrao_despesas TEXT,
    qtd_registros TEXT,
    qtd_trimestres TEXT,
    qtd_anos TEXT
);

-- =====================================================
-- 2. IMPORTAR OPERADORAS ATIVAS
-- =====================================================

\COPY stg_operadoras(cnpj, reg_ans, razao_social, modalidade, uf) FROM '/tmp/csvs/operadoras_ativas.csv' WITH (FORMAT CSV, HEADER TRUE, DELIMITER ';', QUOTE '"', ENCODING 'UTF8');

-- Inserir operadoras ATIVAS
INSERT INTO operadoras (reg_ans, cnpj, razao_social, modalidade, uf, status)
SELECT 
    TRIM(reg_ans),
    REGEXP_REPLACE(cnpj, '[^0-9]', '', 'g'),
    TRIM(razao_social),
    NULLIF(TRIM(modalidade), ''),
    NULLIF(TRIM(uf), ''),
    'ATIVA'::VARCHAR(20)
FROM stg_operadoras
WHERE TRIM(reg_ans) IS NOT NULL 
  AND TRIM(reg_ans) != ''
  AND REGEXP_REPLACE(cnpj, '[^0-9]', '', 'g') IS NOT NULL
  AND TRIM(razao_social) IS NOT NULL
ON CONFLICT (reg_ans) DO NOTHING;

-- Limpar staging para operadoras canceladas
DELETE FROM stg_operadoras;

-- =====================================================
-- 3. IMPORTAR OPERADORAS CANCELADAS
-- =====================================================

\COPY stg_operadoras(cnpj, reg_ans, razao_social, modalidade, uf) FROM '/tmp/csvs/operadoras_canceladas.csv' WITH (FORMAT CSV, HEADER TRUE, DELIMITER ';', QUOTE '"', ENCODING 'UTF8');

-- Inserir operadoras CANCELADAS
INSERT INTO operadoras (reg_ans, cnpj, razao_social, modalidade, uf, status)
SELECT 
    TRIM(reg_ans),
    REGEXP_REPLACE(cnpj, '[^0-9]', '', 'g'),
    TRIM(razao_social),
    NULLIF(TRIM(modalidade), ''),
    NULLIF(TRIM(uf), ''),
    'CANCELADAS'::VARCHAR(20)
FROM stg_operadoras
WHERE TRIM(reg_ans) IS NOT NULL 
  AND TRIM(reg_ans) != ''
  AND REGEXP_REPLACE(cnpj, '[^0-9]', '', 'g') IS NOT NULL
  AND TRIM(razao_social) IS NOT NULL
ON CONFLICT (reg_ans) DO NOTHING;

-- =====================================================
-- 4. IMPORTAR CONSOLIDADOS (SEM DEDUÇÕES)
-- =====================================================

\COPY stg_sinistro_sem(reg_ans, cnpj, razao_social, trimestre, ano, valor_despesas) FROM '/tmp/csvs/sinistro_sem_deducoes.csv' WITH (FORMAT CSV, HEADER TRUE, DELIMITER ';', QUOTE '"', ENCODING 'UTF8');

INSERT INTO consolidados_despesas (cnpj, razao_social, trimestre, ano, valor_despesas, reg_ans)
SELECT
    COALESCE(NULLIF(REGEXP_REPLACE(s.cnpj, '[^0-9]', '', 'g'), ''), o.cnpj, '00000000000000') AS cnpj,
    COALESCE(NULLIF(TRIM(s.razao_social), ''), o.razao_social, s.reg_ans) AS razao_social,
    CAST(TRIM(s.trimestre) AS INTEGER) AS trimestre,
    CAST(TRIM(s.ano) AS INTEGER) AS ano,
    CAST(REPLACE(REPLACE(COALESCE(s.valor_despesas, '0'), '.', ''), ',', '.') AS NUMERIC(18,2)) AS valor_despesas,
    TRIM(s.reg_ans) AS reg_ans
FROM stg_sinistro_sem s
LEFT JOIN operadoras o ON TRIM(s.reg_ans) = o.reg_ans
WHERE TRIM(s.reg_ans) IS NOT NULL 
  AND TRIM(s.reg_ans) != ''
  AND TRIM(s.trimestre) IS NOT NULL
  AND TRIM(s.ano) IS NOT NULL
ON CONFLICT DO NOTHING;

-- =====================================================
-- 5. IMPORTAR CONSOLIDADOS (COM DEDUÇÕES)
-- =====================================================

\COPY stg_sinistro_com(cnpj, razao_social, trimestre, ano, valor_despesas, reg_ans, conta_contabil, descricao) FROM '/tmp/csvs/consolidado_despesas_sinistros_c_deducoes.csv' WITH (FORMAT CSV, HEADER TRUE, DELIMITER ';', QUOTE '"', ENCODING 'UTF8');

INSERT INTO consolidados_despesas_c_deducoes (cnpj, razao_social, trimestre, ano, valor_despesas, reg_ans, descricao)
SELECT
    COALESCE(NULLIF(REGEXP_REPLACE(s.cnpj, '[^0-9]', '', 'g'), ''), o.cnpj, '00000000000000') AS cnpj,
    COALESCE(NULLIF(TRIM(s.razao_social), ''), o.razao_social, s.reg_ans) AS razao_social,
    CAST(TRIM(s.trimestre) AS INTEGER) AS trimestre,
    CAST(TRIM(s.ano) AS INTEGER) AS ano,
    CAST(REPLACE(REPLACE(COALESCE(s.valor_despesas, '0'), '.', ''), ',', '.') AS NUMERIC(18,2)) AS valor_despesas,
    TRIM(s.reg_ans) AS reg_ans,
    NULLIF(TRIM(s.descricao), '') AS descricao
FROM stg_sinistro_com s
LEFT JOIN operadoras o ON TRIM(s.reg_ans) = o.reg_ans
WHERE TRIM(s.reg_ans) IS NOT NULL 
  AND TRIM(s.reg_ans) != ''
  AND TRIM(s.trimestre) IS NOT NULL
  AND TRIM(s.ano) IS NOT NULL
ON CONFLICT DO NOTHING;

-- =====================================================
-- 6. IMPORTAR DESPESAS AGREGADAS (SEM DEDUÇÃO)
-- =====================================================

\COPY stg_despesas_agregadas(razao_social, uf, total_despesas, media_despesas_trimestre, desvio_padrao_despesas, qtd_registros, qtd_trimestres, qtd_anos) FROM '/tmp/csvs/despesas_agregadas.csv' WITH (FORMAT CSV, HEADER TRUE, DELIMITER ';', QUOTE '"', ENCODING 'UTF8');

INSERT INTO despesas_agregadas (razao_social, uf, total_despesas, media_despesas_trimestre, desvio_padrao_despesas, qtd_registros, qtd_trimestres, qtd_anos, reg_ans)
SELECT
    TRIM(s.razao_social) AS razao_social,
    NULLIF(TRIM(s.uf), '') AS uf,
    CAST(REPLACE(REPLACE(COALESCE(s.total_despesas, '0'), '.', ''), ',', '.') AS NUMERIC(18,2)) AS total_despesas,
    CAST(REPLACE(REPLACE(COALESCE(s.media_despesas_trimestre, '0'), '.', ''), ',', '.') AS NUMERIC(18,2)) AS media_despesas_trimestre,
    CAST(REPLACE(REPLACE(COALESCE(s.desvio_padrao_despesas, '0'), '.', ''), ',', '.') AS NUMERIC(18,2)) AS desvio_padrao_despesas,
    CAST(COALESCE(NULLIF(TRIM(s.qtd_registros), ''), '0') AS INTEGER) AS qtd_registros,
    CAST(COALESCE(NULLIF(TRIM(s.qtd_trimestres), ''), '0') AS INTEGER) AS qtd_trimestres,
    CAST(COALESCE(NULLIF(TRIM(s.qtd_anos), ''), '0') AS INTEGER) AS qtd_anos,
    MIN(o.reg_ans) AS reg_ans
FROM stg_despesas_agregadas s
JOIN operadoras o 
  ON o.razao_social = TRIM(s.razao_social)
 AND (o.uf = NULLIF(TRIM(s.uf), '') OR (o.uf IS NULL AND NULLIF(TRIM(s.uf), '') IS NULL))
GROUP BY
    TRIM(s.razao_social),
    NULLIF(TRIM(s.uf), ''),
    s.total_despesas,
    s.media_despesas_trimestre,
    s.desvio_padrao_despesas,
    s.qtd_registros,
    s.qtd_trimestres,
    s.qtd_anos
ON CONFLICT DO NOTHING;

-- =====================================================
-- 7. IMPORTAR DESPESAS AGREGADAS (COM DEDUÇÃO)
-- =====================================================

\COPY stg_despesas_agregadas_c_deducoes(razao_social, uf, total_despesas, media_despesas_trimestre, desvio_padrao_despesas, qtd_registros, qtd_trimestres, qtd_anos) FROM '/tmp/csvs/despesas_agregadas_c_deducoes.csv' WITH (FORMAT CSV, HEADER TRUE, DELIMITER ';', QUOTE '"', ENCODING 'UTF8');

INSERT INTO despesas_agregadas_c_deducoes (razao_social, uf, total_despesas, media_despesas_trimestre, desvio_padrao_despesas, qtd_registros, qtd_trimestres, qtd_anos, reg_ans)
SELECT
    TRIM(s.razao_social) AS razao_social,
    NULLIF(TRIM(s.uf), '') AS uf,
    CAST(REPLACE(REPLACE(COALESCE(s.total_despesas, '0'), '.', ''), ',', '.') AS NUMERIC(18,2)) AS total_despesas,
    CAST(REPLACE(REPLACE(COALESCE(s.media_despesas_trimestre, '0'), '.', ''), ',', '.') AS NUMERIC(18,2)) AS media_despesas_trimestre,
    CAST(REPLACE(REPLACE(COALESCE(s.desvio_padrao_despesas, '0'), '.', ''), ',', '.') AS NUMERIC(18,2)) AS desvio_padrao_despesas,
    CAST(COALESCE(NULLIF(TRIM(s.qtd_registros), ''), '0') AS INTEGER) AS qtd_registros,
    CAST(COALESCE(NULLIF(TRIM(s.qtd_trimestres), ''), '0') AS INTEGER) AS qtd_trimestres,
    CAST(COALESCE(NULLIF(TRIM(s.qtd_anos), ''), '0') AS INTEGER) AS qtd_anos,
    MIN(o.reg_ans) AS reg_ans
FROM stg_despesas_agregadas_c_deducoes s
JOIN operadoras o 
  ON o.razao_social = TRIM(s.razao_social)
 AND (o.uf = NULLIF(TRIM(s.uf), '') OR (o.uf IS NULL AND NULLIF(TRIM(s.uf), '') IS NULL))
GROUP BY
    TRIM(s.razao_social),
    NULLIF(TRIM(s.uf), ''),
    s.total_despesas,
    s.media_despesas_trimestre,
    s.desvio_padrao_despesas,
    s.qtd_registros,
    s.qtd_trimestres,
    s.qtd_anos
ON CONFLICT DO NOTHING;

-- =====================================================
-- 8. VERIFICAÇÃO DOS DADOS IMPORTADOS
-- =====================================================

SELECT 'OPERADORAS' as tabela, COUNT(*) as total FROM operadoras
UNION ALL
SELECT 'DESPESAS AGREGADAS SEM DEDUÇÃO', COUNT(*) FROM despesas_agregadas
UNION ALL
SELECT 'DESPESAS AGREGADAS COM DEDUÇÃO', COUNT(*) FROM despesas_agregadas_c_deducoes
UNION ALL
SELECT 'CONSOLIDADOS SEM DEDUÇÃO', COUNT(*) FROM consolidados_despesas
UNION ALL
SELECT 'CONSOLIDADOS COM DEDUÇÃO', COUNT(*) FROM consolidados_despesas_c_deducoes;

SELECT status, COUNT(*) FROM operadoras GROUP BY status;

SELECT trimestre, COUNT(*), ROUND(SUM(valor_despesas)::numeric, 2) as total FROM consolidados_despesas GROUP BY trimestre ORDER BY trimestre;

-- Reabilitar FK constraints
ALTER TABLE consolidados_despesas ENABLE TRIGGER ALL;
ALTER TABLE consolidados_despesas_c_deducoes ENABLE TRIGGER ALL;
ALTER TABLE despesas_agregadas ENABLE TRIGGER ALL;
ALTER TABLE despesas_agregadas_c_deducoes ENABLE TRIGGER ALL;
