-- =====================================================
-- IMPORTAÇÃO DE DADOS DOS CSVs usando COPY
-- =====================================================

-- Desabilitar FK constraints temporariamente
ALTER TABLE consolidados_despesas DISABLE TRIGGER ALL;
ALTER TABLE consolidados_despesas_c_deducoes DISABLE TRIGGER ALL;
ALTER TABLE despesas_agregadas DISABLE TRIGGER ALL;
ALTER TABLE despesas_agregadas_c_deducoes DISABLE TRIGGER ALL;

-- =====================================================
-- LIMPAR DADOS ANTIGOS (EXCETO OPERADORAS)
-- =====================================================

DELETE FROM consolidados_despesas_c_deducoes;
DELETE FROM consolidados_despesas;
DELETE FROM despesas_agregadas_c_deducoes;
DELETE FROM despesas_agregadas;

-- =====================================================
-- 1. STAGING TABLES
-- =====================================================

DROP TABLE IF EXISTS stg_sinistro_sem CASCADE;
CREATE TEMP TABLE stg_sinistro_sem (
  reg_ans TEXT,
  cnpj TEXT,
  razaosocial TEXT,
  trimestre TEXT,
  ano TEXT,
  valor_de_despesas TEXT
);

DROP TABLE IF EXISTS stg_sinistro_com CASCADE;
CREATE TEMP TABLE stg_sinistro_com (
  cnpj TEXT,
  razaosocial TEXT,
  trimestre TEXT,
  ano TEXT,
  valor_de_despesas TEXT,
  registro_ans TEXT,
  conta_contabil TEXT,
  descricao TEXT
);

DROP TABLE IF EXISTS stg_despesas_agregadas CASCADE;
CREATE TEMP TABLE stg_despesas_agregadas (
  cnpj TEXT,
  razao_social TEXT,
  reg_ans TEXT,
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
  cnpj TEXT,
  razao_social TEXT,
  reg_ans TEXT,
  uf TEXT,
  total_despesas TEXT,
  media_despesas_trimestre TEXT,
  desvio_padrao_despesas TEXT,
  qtd_registros TEXT,
  qtd_trimestres TEXT,
  qtd_anos TEXT
);

-- =====================================================
-- 2. IMPORTAR CONSOLIDADOS (SEM DEDUÇÕES)
-- =====================================================

\COPY stg_sinistro_sem FROM '/mnt/csvs/sinistro_sem_deducoes.csv' WITH (FORMAT CSV, HEADER TRUE, DELIMITER ';', QUOTE '"', ENCODING 'UTF8');

INSERT INTO consolidados_despesas (cnpj, razao_social, trimestre, ano, valor_despesas, reg_ans)
SELECT
    COALESCE(NULLIF(REGEXP_REPLACE(s.cnpj, '[^0-9]', '', 'g'), ''), o.cnpj, '00000000000000') AS cnpj,
    REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(COALESCE(NULLIF(TRIM(s.razaosocial), ''), o.razao_social, s.reg_ans),
        '├è', 'é'),
        '├Ü', 'ú'),
        '├ç', 'ç'),
        '├í', 'í'),
        '├ñ', 'ñ'),
        '├┤', 'á'),
        '├®', 'ô'),
        '├ô', 'õ') AS razao_social,
    CAST(COALESCE(NULLIF(REGEXP_REPLACE(TRIM(s.trimestre), '[^0-9]', '', 'g'), ''), '1') AS INTEGER) AS trimestre,
    CAST(COALESCE(NULLIF(TRIM(s.ano), ''), '2000') AS INTEGER) AS ano,
    CAST(REPLACE(REPLACE(COALESCE(s.valor_de_despesas, '0'), '.', ''), ',', '.') AS NUMERIC(18,2)) AS valor_despesas,
    COALESCE(NULLIF(TRIM(s.reg_ans), ''), '00000') AS reg_ans
FROM stg_sinistro_sem s
LEFT JOIN operadoras o ON TRIM(s.reg_ans) = o.reg_ans
ON CONFLICT DO NOTHING;

-- =====================================================
-- 3. IMPORTAR CONSOLIDADOS (COM DEDUÇÕES)
-- =====================================================

\COPY stg_sinistro_com FROM '/mnt/csvs/consolidado_despesas_sinistros_c_deducoes.csv' WITH (FORMAT CSV, HEADER TRUE, DELIMITER ';', QUOTE '"', ENCODING 'UTF8');

INSERT INTO consolidados_despesas_c_deducoes (cnpj, razao_social, trimestre, ano, valor_despesas, reg_ans, descricao)
SELECT
    COALESCE(NULLIF(REGEXP_REPLACE(s.cnpj, '[^0-9]', '', 'g'), ''), o.cnpj, '00000000000000') AS cnpj,
    REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(COALESCE(NULLIF(TRIM(s.razaosocial), ''), o.razao_social, s.registro_ans),
        '├è', 'é'),
        '├Ü', 'ú'),
        '├ç', 'ç'),
        '├í', 'í'),
        '├ñ', 'ñ'),
        '├┤', 'á'),
        '├®', 'ô'),
        '├ô', 'õ') AS razao_social,
    CAST(COALESCE(NULLIF(REGEXP_REPLACE(TRIM(s.trimestre), '[^0-9]', '', 'g'), ''), '1') AS INTEGER) AS trimestre,
    CAST(COALESCE(NULLIF(TRIM(s.ano), ''), '2000') AS INTEGER) AS ano,
    CAST(REPLACE(REPLACE(COALESCE(s.valor_de_despesas, '0'), '.', ''), ',', '.') AS NUMERIC(18,2)) AS valor_despesas,
    COALESCE(NULLIF(TRIM(s.registro_ans), ''), '00000') AS reg_ans,
    REPLACE(REPLACE(REPLACE(NULLIF(TRIM(s.descricao), ''),
        '├è', 'é'),
        '├Ü', 'ú'),
        '├ç', 'ç') AS descricao
FROM stg_sinistro_com s
LEFT JOIN operadoras o ON TRIM(s.registro_ans) = o.reg_ans
ON CONFLICT DO NOTHING;

-- =====================================================
-- 4. IMPORTAR DESPESAS AGREGADAS (SEM DEDUÇÃO)
-- =====================================================

\COPY stg_despesas_agregadas(cnpj, razao_social, reg_ans, uf, total_despesas, media_despesas_trimestre, desvio_padrao_despesas, qtd_registros, qtd_trimestres, qtd_anos) FROM '/mnt/csvs/despesas_agregadas.csv' WITH (FORMAT CSV, HEADER TRUE, DELIMITER ';', QUOTE '"', ENCODING 'UTF8');

INSERT INTO despesas_agregadas (cnpj, razao_social, uf, total_despesas, media_despesas_trimestre, desvio_padrao_despesas, qtd_registros, qtd_trimestres, qtd_anos, reg_ans)
SELECT
  COALESCE(NULLIF(REGEXP_REPLACE(s.cnpj, '[^0-9]', '', 'g'), ''), o.cnpj, '00000000000000') AS cnpj,
    REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(TRIM(s.razao_social),
        '├è', 'é'),
        '├Ü', 'ú'),
        '├ç', 'ç'),
        '├í', 'í'),
        '├ñ', 'ñ'),
        '├┤', 'á'),
        '├®', 'ô'),
        '├ô', 'õ') AS razao_social,
    NULLIF(TRIM(s.uf), '') AS uf,
    CAST(REPLACE(REPLACE(COALESCE(s.total_despesas, '0'), '.', ''), ',', '.') AS NUMERIC(18,2)) AS total_despesas,
    CAST(REPLACE(REPLACE(COALESCE(s.media_despesas_trimestre, '0'), '.', ''), ',', '.') AS NUMERIC(18,2)) AS media_despesas_trimestre,
    CAST(REPLACE(REPLACE(COALESCE(s.desvio_padrao_despesas, '0'), '.', ''), ',', '.') AS NUMERIC(18,2)) AS desvio_padrao_despesas,
    CAST(COALESCE(NULLIF(TRIM(s.qtd_registros), ''), '0') AS INTEGER) AS qtd_registros,
    CAST(COALESCE(NULLIF(TRIM(s.qtd_trimestres), ''), '0') AS INTEGER) AS qtd_trimestres,
    CAST(COALESCE(NULLIF(TRIM(s.qtd_anos), ''), '0') AS INTEGER) AS qtd_anos,
    COALESCE(o.reg_ans, TRIM(s.reg_ans)) AS reg_ans
FROM stg_despesas_agregadas s
LEFT JOIN operadoras o
  ON o.reg_ans = TRIM(s.reg_ans)
ON CONFLICT DO NOTHING;

-- =====================================================
-- 5. IMPORTAR DESPESAS AGREGADAS (COM DEDUÇÃO)
-- =====================================================

\COPY stg_despesas_agregadas_c_deducoes(cnpj, razao_social, reg_ans, uf, total_despesas, media_despesas_trimestre, desvio_padrao_despesas, qtd_registros, qtd_trimestres, qtd_anos) FROM '/mnt/csvs/despesas_agregadas_c_deducoes.csv' WITH (FORMAT CSV, HEADER TRUE, DELIMITER ';', QUOTE '"', ENCODING 'UTF8');

INSERT INTO despesas_agregadas_c_deducoes (cnpj, razao_social, uf, total_despesas, media_despesas_trimestre, desvio_padrao_despesas, qtd_registros, qtd_trimestres, qtd_anos, reg_ans)
SELECT
  COALESCE(NULLIF(REGEXP_REPLACE(s.cnpj, '[^0-9]', '', 'g'), ''), o.cnpj, '00000000000000') AS cnpj,
    REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(TRIM(s.razao_social),
        '├è', 'é'),
        '├Ü', 'ú'),
        '├ç', 'ç'),
        '├í', 'í'),
        '├ñ', 'ñ'),
        '├┤', 'á'),
        '├®', 'ô'),
        '├ô', 'õ') AS razao_social,
    NULLIF(TRIM(s.uf), '') AS uf,
    CAST(REPLACE(REPLACE(COALESCE(s.total_despesas, '0'), '.', ''), ',', '.') AS NUMERIC(18,2)) AS total_despesas,
    CAST(REPLACE(REPLACE(COALESCE(s.media_despesas_trimestre, '0'), '.', ''), ',', '.') AS NUMERIC(18,2)) AS media_despesas_trimestre,
    CAST(REPLACE(REPLACE(COALESCE(s.desvio_padrao_despesas, '0'), '.', ''), ',', '.') AS NUMERIC(18,2)) AS desvio_padrao_despesas,
    CAST(COALESCE(NULLIF(TRIM(s.qtd_registros), ''), '0') AS INTEGER) AS qtd_registros,
    CAST(COALESCE(NULLIF(TRIM(s.qtd_trimestres), ''), '0') AS INTEGER) AS qtd_trimestres,
    CAST(COALESCE(NULLIF(TRIM(s.qtd_anos), ''), '0') AS INTEGER) AS qtd_anos,
    COALESCE(o.reg_ans, TRIM(s.reg_ans)) AS reg_ans
FROM stg_despesas_agregadas_c_deducoes s
LEFT JOIN operadoras o
  ON o.reg_ans = TRIM(s.reg_ans)
ON CONFLICT DO NOTHING;

-- =====================================================
-- 8. VERIFICAÇÃO DOS DADOS IMPORTADOS
-- =====================================================

SELECT 'OPERADORAS' as tabela, COUNT(*) as total FROM operadoras
UNION ALL
SELECT 'DESPESAS AGREGADAS SEM DEDUCAO', COUNT(*) FROM despesas_agregadas
UNION ALL
SELECT 'DESPESAS AGREGADAS COM DEDUCAO', COUNT(*) FROM despesas_agregadas_c_deducoes
UNION ALL
SELECT 'CONSOLIDADOS SEM DEDUCAO', COUNT(*) FROM consolidados_despesas
UNION ALL
SELECT 'CONSOLIDADOS COM DEDUCAO', COUNT(*) FROM consolidados_despesas_c_deducoes;

SELECT status, COUNT(*) FROM operadoras GROUP BY status;

SELECT trimestre, COUNT(*), ROUND(SUM(valor_despesas)::numeric, 2) as total FROM consolidados_despesas GROUP BY trimestre ORDER BY trimestre;

-- Totais de despesas agregadas
SELECT 'Totais de despesas agregadas:' as info;
SELECT 'SEM DEDUCAO' as tipo, ROUND(SUM(total_despesas)::numeric, 2) as total FROM despesas_agregadas
UNION ALL
SELECT 'COM DEDUCAO' as tipo, ROUND(SUM(total_despesas)::numeric, 2) as total FROM despesas_agregadas_c_deducoes;

-- Reabilitar FK constraints
ALTER TABLE consolidados_despesas ENABLE TRIGGER ALL;
ALTER TABLE consolidados_despesas_c_deducoes ENABLE TRIGGER ALL;
ALTER TABLE despesas_agregadas ENABLE TRIGGER ALL;
ALTER TABLE despesas_agregadas_c_deducoes ENABLE TRIGGER ALL;
