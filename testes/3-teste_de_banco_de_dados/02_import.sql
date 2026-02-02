-- Importação com staging para tratar inconsistências

-- Staging para operadoras
CREATE TABLE IF NOT EXISTS stg_operadoras (
    cnpj          TEXT,
    reg_ans       TEXT,
    razao_social  TEXT,
    modalidade    TEXT,
    uf            TEXT,
    status        TEXT
);

-- Staging para consolidados de despesas SEM dedução
CREATE TABLE IF NOT EXISTS stg_consolidados_despesas (
    cnpj          TEXT,
    razao_social  TEXT,
    trimestre     TEXT,
    ano           TEXT,
    valor_despesas TEXT,
    reg_ans       TEXT
);

-- Staging para consolidados de despesas COM dedução
CREATE TABLE IF NOT EXISTS stg_consolidados_despesas_c_deducoes (
    cnpj          TEXT,
    razao_social  TEXT,
    trimestre     TEXT,
    ano           TEXT,
    valor_despesas TEXT,
    reg_ans       TEXT,
    descricao     TEXT
);

-- Staging para despesas agregadas (antigas)
CREATE TABLE IF NOT EXISTS stg_despesas_consolidadas (
    reg_ans       TEXT,
    razao_social  TEXT,
    uf            TEXT,
    ano           TEXT,
    trimestre     TEXT,
    valor_despesa TEXT
);

-- Tabela de rejeitados para operadoras
CREATE TABLE IF NOT EXISTS rejeitados_operadoras (
    cnpj          TEXT,
    reg_ans       TEXT,
    razao_social  TEXT,
    modalidade    TEXT,
    uf            TEXT,
    status        TEXT,
    motivo        TEXT,
    data_carga    TIMESTAMP DEFAULT NOW()
);

-- Tabela de rejeitados para consolidados
CREATE TABLE IF NOT EXISTS rejeitados_consolidados (
    cnpj          TEXT,
    razao_social  TEXT,
    trimestre     TEXT,
    ano           TEXT,
    valor_despesas TEXT,
    reg_ans       TEXT,
    descricao     TEXT,
    motivo        TEXT,
    data_carga    TIMESTAMP DEFAULT NOW()
);

-- Tabela de rejeitados (antigas despesas agregadas)
CREATE TABLE IF NOT EXISTS rejeitados_despesas (
    reg_ans       TEXT,
    razao_social  TEXT,
    uf            TEXT,
    ano           TEXT,
    trimestre     TEXT,
    valor_despesa TEXT,
    motivo        TEXT,
    data_carga    TIMESTAMP DEFAULT NOW()
);

-- Rejeitar operadoras inválidas
INSERT INTO rejeitados_operadoras (cnpj, reg_ans, razao_social, modalidade, uf, status, motivo)
SELECT
    cnpj, reg_ans, razao_social, modalidade, uf, status,
    CASE
        WHEN NULLIF(TRIM(reg_ans), '') IS NULL THEN 'REG_ANS nulo/vazio'
        WHEN NULLIF(REGEXP_REPLACE(cnpj, '[^0-9]', '', 'g'), '') IS NULL THEN 'CNPJ nulo/vazio'
        WHEN NULLIF(TRIM(razao_social), '') IS NULL THEN 'RAZAO_SOCIAL nula/vazia'
        WHEN NULLIF(TRIM(status), '') IS NULL THEN 'STATUS nulo/vazio'
        ELSE 'OUTRO'
    END
FROM stg_operadoras
WHERE NULLIF(TRIM(reg_ans), '') IS NULL
   OR NULLIF(REGEXP_REPLACE(cnpj, '[^0-9]', '', 'g'), '') IS NULL
   OR NULLIF(TRIM(razao_social), '') IS NULL
   OR NULLIF(TRIM(status), '') IS NULL;

-- Normalização e validação de operadoras (apenas campos obrigatórios)
INSERT INTO operadoras (reg_ans, cnpj, razao_social, modalidade, uf, status)
SELECT
    TRIM(reg_ans) AS reg_ans,
    REGEXP_REPLACE(cnpj, '[^0-9]', '', 'g') AS cnpj,
    TRIM(razao_social) AS razao_social,
    NULLIF(TRIM(modalidade), '') AS modalidade,
    NULLIF(TRIM(uf), '') AS uf,
    TRIM(status) AS status
FROM stg_operadoras
WHERE NULLIF(TRIM(reg_ans), '') IS NOT NULL
  AND NULLIF(REGEXP_REPLACE(cnpj, '[^0-9]', '', 'g'), '') IS NOT NULL
  AND NULLIF(TRIM(razao_social), '') IS NOT NULL
  AND NULLIF(TRIM(status), '') IS NOT NULL
ON CONFLICT (reg_ans) DO UPDATE
SET cnpj = EXCLUDED.cnpj,
    razao_social = EXCLUDED.razao_social,
    modalidade = EXCLUDED.modalidade,
    uf = EXCLUDED.uf,
    status = EXCLUDED.status;

-- =====================================================
-- CONSOLIDADOS SEM DEDUÇÃO: Validação completa (todos campos NOT NULL)
-- =====================================================

-- Rejeitar consolidados inválidos
INSERT INTO rejeitados_consolidados (cnpj, razao_social, trimestre, ano, valor_despesas, reg_ans, descricao, motivo)
SELECT
    cnpj, razao_social, trimestre, ano, valor_despesas, reg_ans, NULL,
    CASE
        WHEN NULLIF(REGEXP_REPLACE(cnpj, '[^0-9]', '', 'g'), '') IS NULL THEN 'CNPJ nulo/vazio'
        WHEN NULLIF(TRIM(razao_social), '') IS NULL THEN 'RAZAO_SOCIAL nula/vazia'
        WHEN NULLIF(TRIM(trimestre), '') IS NULL THEN 'TRIMESTRE nulo/vazio'
        WHEN TRIM(trimestre) !~ '^[1-4]$' THEN 'TRIMESTRE inválido (deve ser 1-4)'
        WHEN NULLIF(TRIM(ano), '') IS NULL THEN 'ANO nulo/vazio'
        WHEN TRIM(ano) !~ '^[0-9]{4}$' THEN 'ANO inválido'
        WHEN NULLIF(TRIM(valor_despesas), '') IS NULL THEN 'VALOR_DESPESAS nulo/vazio'
        WHEN REGEXP_REPLACE(valor_despesas, '[^0-9,.-]', '', 'g') !~ '^-?[0-9]+([,\.][0-9]+)?$' THEN 'VALOR_DESPESAS formato inválido'
        WHEN NULLIF(TRIM(reg_ans), '') IS NULL THEN 'REG_ANS nulo/vazio'
        ELSE 'OUTRO'
    END
FROM stg_consolidados_despesas
WHERE NULLIF(REGEXP_REPLACE(cnpj, '[^0-9]', '', 'g'), '') IS NULL
   OR NULLIF(TRIM(razao_social), '') IS NULL
   OR NULLIF(TRIM(trimestre), '') IS NULL
   OR TRIM(trimestre) !~ '^[1-4]$'
   OR NULLIF(TRIM(ano), '') IS NULL
   OR TRIM(ano) !~ '^[0-9]{4}$'
   OR NULLIF(TRIM(valor_despesas), '') IS NULL
   OR REGEXP_REPLACE(valor_despesas, '[^0-9,.-]', '', 'g') !~ '^-?[0-9]+([,\.][0-9]+)?$'
   OR NULLIF(TRIM(reg_ans), '') IS NULL;

-- Inserir consolidados SEM dedução válidos
INSERT INTO consolidados_despesas (cnpj, razao_social, trimestre, ano, valor_despesas, reg_ans)
SELECT
    REGEXP_REPLACE(cnpj, '[^0-9]', '', 'g') AS cnpj,
    TRIM(razao_social) AS razao_social,
    CAST(TRIM(trimestre) AS INTEGER) AS trimestre,
    CAST(TRIM(ano) AS INTEGER) AS ano,
    CAST(REPLACE(REGEXP_REPLACE(valor_despesas, '[^0-9,.-]', '', 'g'), ',', '.') AS NUMERIC(18,2)) AS valor_despesas,
    TRIM(reg_ans) AS reg_ans
FROM stg_consolidados_despesas
WHERE NULLIF(REGEXP_REPLACE(cnpj, '[^0-9]', '', 'g'), '') IS NOT NULL
  AND NULLIF(TRIM(razao_social), '') IS NOT NULL
  AND NULLIF(TRIM(trimestre), '') IS NOT NULL
  AND TRIM(trimestre) ~ '^[1-4]$'
  AND NULLIF(TRIM(ano), '') IS NOT NULL
  AND TRIM(ano) ~ '^[0-9]{4}$'
  AND NULLIF(TRIM(valor_despesas), '') IS NOT NULL
  AND REGEXP_REPLACE(valor_despesas, '[^0-9,.-]', '', 'g') ~ '^-?[0-9]+([,\.][0-9]+)?$'
  AND NULLIF(TRIM(reg_ans), '') IS NOT NULL;

-- =====================================================
-- CONSOLIDADOS COM DEDUÇÃO: Validação (descricao é opcional)
-- =====================================================

-- Rejeitar consolidados COM dedução inválidos
INSERT INTO rejeitados_consolidados (cnpj, razao_social, trimestre, ano, valor_despesas, reg_ans, descricao, motivo)
SELECT
    cnpj, razao_social, trimestre, ano, valor_despesas, reg_ans, descricao,
    CASE
        WHEN NULLIF(REGEXP_REPLACE(cnpj, '[^0-9]', '', 'g'), '') IS NULL THEN 'CNPJ nulo/vazio'
        WHEN NULLIF(TRIM(razao_social), '') IS NULL THEN 'RAZAO_SOCIAL nula/vazia'
        WHEN NULLIF(TRIM(trimestre), '') IS NULL THEN 'TRIMESTRE nulo/vazio'
        WHEN TRIM(trimestre) !~ '^[1-4]$' THEN 'TRIMESTRE inválido (deve ser 1-4)'
        WHEN NULLIF(TRIM(ano), '') IS NULL THEN 'ANO nulo/vazio'
        WHEN TRIM(ano) !~ '^[0-9]{4}$' THEN 'ANO inválido'
        WHEN NULLIF(TRIM(valor_despesas), '') IS NULL THEN 'VALOR_DESPESAS nulo/vazio'
        WHEN REGEXP_REPLACE(valor_despesas, '[^0-9,.-]', '', 'g') !~ '^-?[0-9]+([,\.][0-9]+)?$' THEN 'VALOR_DESPESAS formato inválido'
        WHEN NULLIF(TRIM(reg_ans), '') IS NULL THEN 'REG_ANS nulo/vazio'
        ELSE 'OUTRO'
    END
FROM stg_consolidados_despesas_c_deducoes
WHERE NULLIF(REGEXP_REPLACE(cnpj, '[^0-9]', '', 'g'), '') IS NULL
   OR NULLIF(TRIM(razao_social), '') IS NULL
   OR NULLIF(TRIM(trimestre), '') IS NULL
   OR TRIM(trimestre) !~ '^[1-4]$'
   OR NULLIF(TRIM(ano), '') IS NULL
   OR TRIM(ano) !~ '^[0-9]{4}$'
   OR NULLIF(TRIM(valor_despesas), '') IS NULL
   OR REGEXP_REPLACE(valor_despesas, '[^0-9,.-]', '', 'g') !~ '^-?[0-9]+([,\.][0-9]+)?$'
   OR NULLIF(TRIM(reg_ans), '') IS NULL;

-- Inserir consolidados COM dedução válidos (descricao pode ser NULL)
INSERT INTO consolidados_despesas_c_deducoes (cnpj, razao_social, trimestre, ano, valor_despesas, reg_ans, descricao)
SELECT
    REGEXP_REPLACE(cnpj, '[^0-9]', '', 'g') AS cnpj,
    TRIM(razao_social) AS razao_social,
    CAST(TRIM(trimestre) AS INTEGER) AS trimestre,
    CAST(TRIM(ano) AS INTEGER) AS ano,
    CAST(REPLACE(REGEXP_REPLACE(valor_despesas, '[^0-9,.-]', '', 'g'), ',', '.') AS NUMERIC(18,2)) AS valor_despesas,
    TRIM(reg_ans) AS reg_ans,
    NULLIF(TRIM(descricao), '') AS descricao
FROM stg_consolidados_despesas_c_deducoes
WHERE NULLIF(REGEXP_REPLACE(cnpj, '[^0-9]', '', 'g'), '') IS NOT NULL
  AND NULLIF(TRIM(razao_social), '') IS NOT NULL
  AND NULLIF(TRIM(trimestre), '') IS NOT NULL
  AND TRIM(trimestre) ~ '^[1-4]$'
  AND NULLIF(TRIM(ano), '') IS NOT NULL
  AND TRIM(ano) ~ '^[0-9]{4}$'
  AND NULLIF(TRIM(valor_despesas), '') IS NOT NULL
  AND REGEXP_REPLACE(valor_despesas, '[^0-9,.-]', '', 'g') ~ '^-?[0-9]+([,\.][0-9]+)?$'
  AND NULLIF(TRIM(reg_ans), '') IS NOT NULL;

-- Identificar registros inválidos das despesas
INSERT INTO rejeitados_despesas (reg_ans, razao_social, uf, ano, trimestre, valor_despesa, motivo)
SELECT
    reg_ans, razao_social, uf, ano, trimestre, valor_despesa,
    CASE
        WHEN NULLIF(TRIM(reg_ans), '') IS NULL THEN 'REG_ANS nulo'
        WHEN NULLIF(TRIM(razao_social), '') IS NULL THEN 'RAZAO_SOCIAL nula'
        WHEN NULLIF(TRIM(ano), '') IS NULL THEN 'ANO nulo'
        WHEN NULLIF(TRIM(trimestre), '') IS NULL THEN 'TRIMESTRE nulo'
        WHEN NULLIF(TRIM(valor_despesa), '') IS NULL THEN 'VALOR nulo'
        WHEN REGEXP_REPLACE(valor_despesa, '[^0-9,.-]', '', 'g') !~ '^-?[0-9]+([,\.][0-9]+)?$' THEN 'VALOR inválido'
        ELSE 'OUTRO'
    END
FROM stg_despesas_consolidadas
WHERE NULLIF(TRIM(reg_ans), '') IS NULL
   OR NULLIF(TRIM(razao_social), '') IS NULL
   OR NULLIF(TRIM(ano), '') IS NULL
   OR NULLIF(TRIM(trimestre), '') IS NULL
   OR NULLIF(TRIM(valor_despesa), '') IS NULL
   OR REGEXP_REPLACE(valor_despesa, '[^0-9,.-]', '', 'g') !~ '^-?[0-9]+([,\.][0-9]+)?$';

-- Inserção limpa nas despesas consolidadas
INSERT INTO despesas_consolidadas (reg_ans, razao_social, uf, ano, trimestre, valor_despesa)
SELECT
    TRIM(reg_ans) AS reg_ans,
    TRIM(razao_social) AS razao_social,
    NULLIF(TRIM(uf), '') AS uf,
    CAST(NULLIF(TRIM(ano), '') AS SMALLINT) AS ano,
    CAST(NULLIF(TRIM(trimestre), '') AS SMALLINT) AS trimestre,
    CAST(REPLACE(REGEXP_REPLACE(valor_despesa, '[^0-9,.-]', '', 'g'), ',', '.') AS NUMERIC(18,2)) AS valor_despesa
FROM stg_despesas_consolidadas
WHERE NULLIF(TRIM(reg_ans), '') IS NOT NULL
  AND NULLIF(TRIM(razao_social), '') IS NOT NULL
  AND NULLIF(TRIM(ano), '') IS NOT NULL
  AND NULLIF(TRIM(trimestre), '') IS NOT NULL
  AND NULLIF(TRIM(valor_despesa), '') IS NOT NULL
  AND REGEXP_REPLACE(valor_despesa, '[^0-9,.-]', '', 'g') ~ '^-?[0-9]+([,\.][0-9]+)?$';
