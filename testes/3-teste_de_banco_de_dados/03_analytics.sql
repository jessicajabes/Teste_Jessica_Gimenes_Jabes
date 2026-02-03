-- QUERIES ANALÍTICAS - TESTE 3
-- Análise de Operadoras de Saúde Suplementar

-- ============================================================================
-- Query 1: Top 5 Operadoras com Maior Crescimento Percentual
-- ============================================================================

WITH base AS (
    SELECT 'SEM DEDUÇÃO' AS tipo_despesa, reg_ans, ano, trimestre, SUM(valor_despesas) AS valor_trim
    FROM consolidados_despesas
    WHERE valor_despesas > 0
    GROUP BY reg_ans, ano, trimestre
    
    UNION ALL
    
    SELECT 'COM DEDUÇÃO' AS tipo_despesa, reg_ans, ano, trimestre, SUM(valor_despesas) AS valor_trim
    FROM consolidados_despesas_c_deducoes
    WHERE valor_despesas > 0
    GROUP BY reg_ans, ano, trimestre
),
serie_temporal AS (
    SELECT
        tipo_despesa,
        reg_ans,
        valor_trim,
        FIRST_VALUE(valor_trim) OVER (PARTITION BY tipo_despesa, reg_ans ORDER BY ano, trimestre) AS valor_inicial,
        LAST_VALUE(valor_trim) OVER (PARTITION BY tipo_despesa, reg_ans ORDER BY ano, trimestre ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS valor_final,
        COUNT(*) OVER (PARTITION BY tipo_despesa, reg_ans) AS qtd_periodos
    FROM base
),
crescimento AS (
    SELECT DISTINCT
        tipo_despesa,
        s.reg_ans,
        op.razao_social,
        CASE
            WHEN valor_inicial = 0 THEN NULL
            ELSE ROUND(((valor_final - valor_inicial) / valor_inicial) * 100, 2)
        END AS crescimento_percentual,
        ROW_NUMBER() OVER (PARTITION BY tipo_despesa ORDER BY ((valor_final - valor_inicial) / valor_inicial) DESC) AS rn
    FROM serie_temporal s
    LEFT JOIN operadoras op ON s.reg_ans = op.reg_ans
    WHERE qtd_periodos > 1
)
SELECT tipo_despesa, razao_social, reg_ans, crescimento_percentual
FROM crescimento
WHERE rn <= 5
ORDER BY tipo_despesa, crescimento_percentual DESC;


-- ============================================================================
-- Query 2: Distribuição de Despesas por UF (Top 5 por tipo)
-- ============================================================================

WITH despesas_por_uf AS (
    SELECT 'SEM DEDUÇÃO' AS tipo_despesa, o.uf, cd.reg_ans, SUM(cd.valor_despesas) AS total
    FROM consolidados_despesas cd
    LEFT JOIN operadoras o ON cd.reg_ans = o.reg_ans
    GROUP BY o.uf, cd.reg_ans
    
    UNION ALL
    
    SELECT 'COM DEDUÇÃO' AS tipo_despesa, o.uf, cd.reg_ans, SUM(cd.valor_despesas) AS total
    FROM consolidados_despesas_c_deducoes cd
    LEFT JOIN operadoras o ON cd.reg_ans = o.reg_ans
    GROUP BY o.uf, cd.reg_ans
),
agregado AS (
    SELECT
        tipo_despesa,
        COALESCE(uf, 'N/D') AS uf,
        ROUND(SUM(total), 2) AS total_despesas_uf,
        COUNT(DISTINCT reg_ans) AS qtd_operadoras,
        ROUND(AVG(total), 2) AS media_despesas_por_operadora,
        ROW_NUMBER() OVER (PARTITION BY tipo_despesa ORDER BY SUM(total) DESC) AS rn
    FROM despesas_por_uf
    GROUP BY tipo_despesa, uf
)
SELECT tipo_despesa, uf, total_despesas_uf, qtd_operadoras, media_despesas_por_operadora
FROM agregado
WHERE rn <= 5
ORDER BY tipo_despesa, total_despesas_uf DESC;


-- ============================================================================
-- Query 3: Operadoras Acima da Média em 2+ Trimestres
-- ============================================================================

WITH base AS (
    SELECT 'SEM DEDUÇÃO' AS tipo_despesa, reg_ans, ano, trimestre, SUM(valor_despesas) AS total_trim
    FROM consolidados_despesas
    GROUP BY reg_ans, ano, trimestre
    
    UNION ALL
    
    SELECT 'COM DEDUÇÃO' AS tipo_despesa, reg_ans, ano, trimestre, SUM(valor_despesas) AS total_trim
    FROM consolidados_despesas_c_deducoes
    GROUP BY reg_ans, ano, trimestre
),
media_por_tipo AS (
    SELECT tipo_despesa, AVG(total_trim) AS media_trim
    FROM base
    GROUP BY tipo_despesa
),
trimestres_acima_media AS (
    SELECT
        b.tipo_despesa,
        b.reg_ans,
        COUNT(*) AS qtd_trimestres_acima_media
    FROM base b
    INNER JOIN media_por_tipo m ON b.tipo_despesa = m.tipo_despesa
    WHERE b.total_trim > m.media_trim
    GROUP BY b.tipo_despesa, b.reg_ans
    HAVING COUNT(*) >= 2
)
SELECT
    tipo_despesa,
    COUNT(DISTINCT reg_ans) AS total_operadoras_acima_media
FROM trimestres_acima_media
GROUP BY tipo_despesa
ORDER BY tipo_despesa;


