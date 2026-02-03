-- IMPORT DE OPERADORAS - SQL PURO
-- Usando arquivo CSV já processado e limpo

DELETE FROM consolidados_despesas_c_deducoes;
DELETE FROM consolidados_despesas;
DELETE FROM despesas_agregadas_c_deducoes;
DELETE FROM despesas_agregadas;
DELETE FROM operadoras;

\echo '>>> Importando 4.156 operadoras...'

\copy operadoras (reg_ans, cnpj, razao_social, modalidade, uf, status) FROM '/mnt/csvs/operadoras_clean.csv' WITH (FORMAT csv, DELIMITER ';', HEADER true, ENCODING 'UTF8', QUOTE '"', ESCAPE '"');

-- Corrigir encoding de caracteres acentuados corrompidos
UPDATE operadoras SET 
    razao_social = REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(razao_social,
        '├è', 'é'),
        '├Ü', 'ú'),
        '├ç', 'ç'),
        '├í', 'í'),
        '├ñ', 'ñ'),
        '├┤', 'á'),
        '├®', 'ô'),
        '├ô', 'õ'),
    modalidade = REPLACE(REPLACE(REPLACE(modalidade, '├è', 'é'), '├Ü', 'ú'), '├ç', 'ç')
WHERE razao_social LIKE '%├%' OR modalidade LIKE '%├%';

\echo ''
\echo '[OK] IMPORTACAO CONCLUIDA'
\echo ''

SELECT 'Total: ' || COUNT(*) FROM operadoras;

SELECT 
    status,
    COUNT(*) as total
FROM operadoras
GROUP BY status
ORDER BY status DESC;
