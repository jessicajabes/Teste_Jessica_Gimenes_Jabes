-- =====================================================
-- QUERIES ANALÍTICAS - TESTE 3
-- Análise de Operadoras de Saúde Suplementar
-- =====================================================

-- =====================================================
-- Query 1: Top 5 Operadoras com Maior Crescimento Percentual
-- =====================================================
-- DESAFIO: Operadoras podem não ter dados em todos os trimestres
-- SOLUÇÃO: Comparar primeiro e último trimestre DISPONÍVEL por operadora
-- JUSTIFICATIVA: 
--   - Permite avaliar crescimento real mesmo com dados esparsos
--   - Operadoras com apenas 1 trimestre são automaticamente excluídas
--   - Evita assumir valores zero que distorceriam análise
--   - Reflete a realidade de variabilidade nos dados de saúde suplementar

-- VERSÃO SEM DEDUÇÃO
WITH base_sem AS (
    SELECT
        reg_ans,
        razao_social,
        ano,
        trimestre,
        SUM(valor_despesas) AS total_trim
    FROM consolidados_despesas
    GROUP BY reg_ans, razao_social, ano, trimestre
),
periodos_sem AS (
    SELECT
        reg_ans,
        razao_social,
        MIN(ano * 10 + trimestre) AS periodo_inicio,
        MAX(ano * 10 + trimestre) AS periodo_fim
    FROM base_sem
    GROUP BY reg_ans, razao_social
    HAVING MIN(ano * 10 + trimestre) < MAX(ano * 10 + trimestre)
),
valores_sem AS (
    SELECT
        p.reg_ans,
        p.razao_social,
        b1.total_trim AS total_inicio,
        b2.total_trim AS total_fim
    FROM periodos_sem p
    JOIN base_sem b1
      ON b1.reg_ans = p.reg_ans
     AND (b1.ano * 10 + b1.trimestre) = p.periodo_inicio
    JOIN base_sem b2
      ON b2.reg_ans = p.reg_ans
     AND (b2.ano * 10 + b2.trimestre) = p.periodo_fim
),
resultado_sem AS (
    SELECT
        'SEM DEDUÇÃO' AS tipo_despesa,
        reg_ans,
        razao_social,
        ROUND(total_inicio, 2) AS despesas_inicio,
        ROUND(total_fim, 2) AS despesas_fim,
        CASE
            WHEN total_inicio = 0 THEN NULL
            ELSE ROUND(((total_fim - total_inicio) / total_inicio) * 100, 2)
        END AS crescimento_percentual,
        ROW_NUMBER() OVER (ORDER BY CASE WHEN total_inicio = 0 THEN NULL ELSE ROUND(((total_fim - total_inicio) / total_inicio) * 100, 2) END DESC NULLS LAST) AS rn
    FROM valores_sem
    WHERE total_inicio IS NOT NULL AND total_fim IS NOT NULL
),

-- VERSÃO COM DEDUÇÃO
base_com AS (
    SELECT
        reg_ans,
        razao_social,
        ano,
        trimestre,
        SUM(valor_despesas) AS total_trim
    FROM consolidados_despesas_c_deducoes
    GROUP BY reg_ans, razao_social, ano, trimestre
),
periodos_com AS (
    SELECT
        reg_ans,
        razao_social,
        MIN(ano * 10 + trimestre) AS periodo_inicio,
        MAX(ano * 10 + trimestre) AS periodo_fim
    FROM base_com
    GROUP BY reg_ans, razao_social
    HAVING MIN(ano * 10 + trimestre) < MAX(ano * 10 + trimestre)
),
valores_com AS (
    SELECT
        p.reg_ans,
        p.razao_social,
        b1.total_trim AS total_inicio,
        b2.total_trim AS total_fim
    FROM periodos_com p
    JOIN base_com b1
      ON b1.reg_ans = p.reg_ans
     AND (b1.ano * 10 + b1.trimestre) = p.periodo_inicio
    JOIN base_com b2
      ON b2.reg_ans = p.reg_ans
     AND (b2.ano * 10 + b2.trimestre) = p.periodo_fim
),
resultado_com AS (
    SELECT
        'COM DEDUÇÃO' AS tipo_despesa,
        reg_ans,
        razao_social,
        ROUND(total_inicio, 2) AS despesas_inicio,
        ROUND(total_fim, 2) AS despesas_fim,
        CASE
            WHEN total_inicio = 0 THEN NULL
            ELSE ROUND(((total_fim - total_inicio) / total_inicio) * 100, 2)
        END AS crescimento_percentual,
        ROW_NUMBER() OVER (ORDER BY CASE WHEN total_inicio = 0 THEN NULL ELSE ROUND(((total_fim - total_inicio) / total_inicio) * 100, 2) END DESC NULLS LAST) AS rn
    FROM valores_com
    WHERE total_inicio IS NOT NULL AND total_fim IS NOT NULL
)
SELECT tipo_despesa, reg_ans, razao_social, despesas_inicio, despesas_fim, crescimento_percentual 
FROM resultado_sem WHERE rn <= 5
UNION ALL
SELECT tipo_despesa, reg_ans, razao_social, despesas_inicio, despesas_fim, crescimento_percentual 
FROM resultado_com WHERE rn <= 5
ORDER BY tipo_despesa, crescimento_percentual DESC;

-- =====================================================
-- Query 2: Distribuição de Despesas por UF
-- =====================================================
-- DESAFIO ADICIONAL: Calcular média por operadora em cada UF
-- SOLUÇÃO ADOTADA: Usar operadoras.uf como referência + agregação em dois níveis
-- JUSTIFICATIVA:
--   - Operadoras geralmente têm UF registrada na tabela operadoras
--   - Agregamos despesas por UF e calculamos estatísticas
--   - Média por operadora = total_uf / quantidade_operadoras_distintas
--   - Reflete o padrão de gasto por operadora em cada estado

-- VERSÃO SEM DEDUÇÃO
WITH despesas_por_uf_sem AS (
    SELECT
        COALESCE(o.uf, 'N/D') AS uf,
        c.reg_ans,
        SUM(c.valor_despesas) AS total_operadora_uf
    FROM consolidados_despesas c
    LEFT JOIN operadoras o ON c.reg_ans = o.reg_ans
    GROUP BY COALESCE(o.uf, 'N/D'), c.reg_ans
),
agregado_uf_sem AS (
    SELECT
        uf,
        SUM(total_operadora_uf) AS total_uf,
        COUNT(DISTINCT reg_ans) AS qtd_operadoras,
        AVG(total_operadora_uf) AS media_por_operadora
    FROM despesas_por_uf_sem
    GROUP BY uf
),
resultado_uf_sem AS (
    SELECT
        'SEM DEDUÇÃO' AS tipo_despesa,
        uf,
        ROUND(total_uf, 2) AS total_despesas_uf,
        qtd_operadoras,
        ROUND(media_por_operadora, 2) AS media_despesas_por_operadora,
        ROW_NUMBER() OVER (ORDER BY total_uf DESC) AS rn
    FROM agregado_uf_sem
),

-- VERSÃO COM DEDUÇÃO
despesas_por_uf_com AS (
    SELECT
        COALESCE(o.uf, 'N/D') AS uf,
        c.reg_ans,
        SUM(c.valor_despesas) AS total_operadora_uf
    FROM consolidados_despesas_c_deducoes c
    LEFT JOIN operadoras o ON c.reg_ans = o.reg_ans
    GROUP BY COALESCE(o.uf, 'N/D'), c.reg_ans
),
agregado_uf_com AS (
    SELECT
        uf,
        SUM(total_operadora_uf) AS total_uf,
        COUNT(DISTINCT reg_ans) AS qtd_operadoras,
        AVG(total_operadora_uf) AS media_por_operadora
    FROM despesas_por_uf_com
    GROUP BY uf
),
resultado_uf_com AS (
    SELECT
        'COM DEDUÇÃO' AS tipo_despesa,
        uf,
        ROUND(total_uf, 2) AS total_despesas_uf,
        qtd_operadoras,
        ROUND(media_por_operadora, 2) AS media_despesas_por_operadora,
        ROW_NUMBER() OVER (ORDER BY total_uf DESC) AS rn
    FROM agregado_uf_com
)
SELECT tipo_despesa, uf, total_despesas_uf, qtd_operadoras, media_despesas_por_operadora
FROM resultado_uf_sem WHERE rn <= 5
UNION ALL
SELECT tipo_despesa, uf, total_despesas_uf, qtd_operadoras, media_despesas_por_operadora
FROM resultado_uf_com WHERE rn <= 5
ORDER BY tipo_despesa, total_despesas_uf DESC;

-- =====================================================
-- Query 3: Operadoras Acima da Média em 2+ Trimestres
-- =====================================================
-- DESAFIO: Diferentes abordagens disponíveis
-- 
-- OPÇÃO A (ESCOLHIDA): CTEs com agregações progressivas
--   (+) Legibilidade: cada CTE tem responsabilidade clara
--   (+) Manutenibilidade: fácil adicionar filtros ou debug
--   (+) Performance: otimizador pode materializar CTEs
--   (-) Múltiplas passadas nos dados
--
-- OPÇÃO B: Subquery correlacionada
--   (+) Compacto
--   (-) Menos legível, difícil de debug
--   (-) Performance pior em datasets grandes
--
-- OPÇÃO C: Window functions
--   (+) Performance superior em alguns casos
--   (-) Mais complexo para esta lógica específica
--   (-) Menos intuitivo para manutenção
--
-- JUSTIFICATIVA DA ESCOLHA A:
--   1. Auditoria e revisão de código facilitadas
--   2. Cada CTE pode ser executada isoladamente para debug
--   3. Performance aceitável para volume esperado
--   4. Trade-off favorável entre clareza e eficiência

-- VERSÃO SEM DEDUÇÃO
WITH base_sem AS (
    SELECT
        reg_ans,
        razao_social,
        ano,
        trimestre,
        SUM(valor_despesas) AS total_trim
    FROM consolidados_despesas
    GROUP BY reg_ans, razao_social, ano, trimestre
),
media_geral_sem AS (
    SELECT AVG(total_trim) AS media_trim
    FROM base_sem
),
trimestres_acima_media_sem AS (
    SELECT
        b.reg_ans,
        b.razao_social,
        b.ano,
        b.trimestre,
        b.total_trim,
        m.media_trim
    FROM base_sem b
    CROSS JOIN media_geral_sem m
    WHERE b.total_trim > m.media_trim
),
contagem_por_operadora_sem AS (
    SELECT
        reg_ans,
        razao_social,
        COUNT(*) AS qtd_trimestres_acima_media,
        ROUND(AVG(total_trim), 2) AS media_despesas,
        ROW_NUMBER() OVER (ORDER BY COUNT(*) DESC, AVG(total_trim) DESC) AS rn
    FROM trimestres_acima_media_sem
    GROUP BY reg_ans, razao_social
    HAVING COUNT(*) >= 2
),
resultado_sem AS (
    SELECT
        'SEM DEDUÇÃO' AS tipo_despesa,
        reg_ans,
        razao_social,
        qtd_trimestres_acima_media,
        media_despesas
    FROM contagem_por_operadora_sem
),

-- VERSÃO COM DEDUÇÃO
base_com AS (
    SELECT
        reg_ans,
        razao_social,
        ano,
        trimestre,
        SUM(valor_despesas) AS total_trim
    FROM consolidados_despesas_c_deducoes
    GROUP BY reg_ans, razao_social, ano, trimestre
),
media_geral_com AS (
    SELECT AVG(total_trim) AS media_trim
    FROM base_com
),
trimestres_acima_media_com AS (
    SELECT
        b.reg_ans,
        b.razao_social,
        b.ano,
        b.trimestre,
        b.total_trim,
        m.media_trim
    FROM base_com b
    CROSS JOIN media_geral_com m
    WHERE b.total_trim > m.media_trim
),
contagem_por_operadora_com AS (
    SELECT
        reg_ans,
        razao_social,
        COUNT(*) AS qtd_trimestres_acima_media,
        ROUND(AVG(total_trim), 2) AS media_despesas,
        ROW_NUMBER() OVER (ORDER BY COUNT(*) DESC, AVG(total_trim) DESC) AS rn
    FROM trimestres_acima_media_com
    GROUP BY reg_ans, razao_social
    HAVING COUNT(*) >= 2
),
resultado_com AS (
    SELECT
        'COM DEDUÇÃO' AS tipo_despesa,
        reg_ans,
        razao_social,
        qtd_trimestres_acima_media,
        media_despesas
    FROM contagem_por_operadora_com
)
SELECT tipo_despesa, reg_ans, razao_social, qtd_trimestres_acima_media, media_despesas
FROM resultado_sem
UNION ALL
SELECT tipo_despesa, reg_ans, razao_social, qtd_trimestres_acima_media, media_despesas
FROM resultado_com
ORDER BY tipo_despesa, qtd_trimestres_acima_media DESC, media_despesas DESC;

-- =====================================================
-- RESUMO ANALÍTICO
-- =====================================================
-- Estatísticas gerais de ambas as versões

SELECT 
    'ESTATÍSTICAS GERAIS - SEM DEDUÇÃO' AS metrica,
    'Operadoras com dados' AS descricao,
    COUNT(DISTINCT reg_ans)::TEXT AS valor
FROM consolidados_despesas
UNION ALL
SELECT 'ESTATÍSTICAS GERAIS - SEM DEDUÇÃO', 'Trimestres analisados', COUNT(DISTINCT trimestre)::TEXT
FROM consolidados_despesas
UNION ALL
SELECT 'ESTATÍSTICAS GERAIS - SEM DEDUÇÃO', 'Total de despesas', ROUND(SUM(valor_despesas), 2)::TEXT
FROM consolidados_despesas
UNION ALL
SELECT 'ESTATÍSTICAS GERAIS - SEM DEDUÇÃO', 'Média por trimestre', ROUND(AVG(valor_despesas), 2)::TEXT
FROM consolidados_despesas
UNION ALL
SELECT 'ESTATÍSTICAS GERAIS - COM DEDUÇÃO', 'Operadoras com dados', COUNT(DISTINCT reg_ans)::TEXT
FROM consolidados_despesas_c_deducoes
UNION ALL
SELECT 'ESTATÍSTICAS GERAIS - COM DEDUÇÃO', 'Trimestres analisados', COUNT(DISTINCT trimestre)::TEXT
FROM consolidados_despesas_c_deducoes
UNION ALL
SELECT 'ESTATÍSTICAS GERAIS - COM DEDUÇÃO', 'Total de despesas', ROUND(SUM(valor_despesas), 2)::TEXT
FROM consolidados_despesas_c_deducoes
UNION ALL
SELECT 'ESTATÍSTICAS GERAIS - COM DEDUÇÃO', 'Média por trimestre', ROUND(AVG(valor_despesas), 2)::TEXT
FROM consolidados_despesas_c_deducoes;
