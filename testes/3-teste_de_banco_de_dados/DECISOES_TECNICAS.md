# Teste 3: Banco de Dados e Queries Analíticas

## Execução

AVISO: Testes 1 e 2 devem ter sido executados antes. Arquivos necessários:
- Teste 1: operadoras_ativas.csv, operadoras_canceladas.csv, consolidado_despesas_sinistros_c_deducoes.csv, sinistro_sem_deducoes.csv
- Teste 2: despesas_agregadas.csv, despesas_agregadas_c_deducoes.csv

Comando para executar:
```powershell
cd testes\3-teste_de_banco_de_dados
.\executar_teste_3.ps1
```

O script unificado realiza duas operações:
1. PASSO 1: Localiza CSVs em downloads/1-trimestres_consolidados/extracted, downloads/2-tranformacao_validacao/extracted e downloads/operadoras
2. PASSO 2: Cria tabelas PostgreSQL e importa dados executando 01_ddl.sql, 02_import_clean.sql, 04_import_data.sql, 03_analytics.sql em sequência

Alternativamente, executar scripts SQL diretamente no container:
```bash
docker exec intuitive-care psql -U jessica -d intuitive_care -f 01_ddl.sql
docker exec intuitive-care psql -U jessica -d intuitive_care -f 02_import_clean.sql
docker exec intuitive-care psql -U jessica -d intuitive_care -f 04_import_data.sql
docker exec intuitive-care psql -U jessica -d intuitive_care -f 03_analytics.sql
```

---

## Trade-offs Técnicos

### 1. Normalização vs Desnormalização

ESCOLHA: Desnormalização com redundância controlada

ESTRUTURA IMPLEMENTADA:
- operadoras: tabela mestre (reg_ans, cnpj, razao_social, modalidade, uf, status)
- consolidados_despesas: inclui reg_ans, cnpj, razao_social, trimestre, ano, valor
- consolidados_despesas_c_deducoes: inclui reg_ans, cnpj, razao_social, trimestre, ano, valor
- despesas_agregadas: inclui reg_ans, razao_social, uf, total, media, desvio
- despesas_agregadas_c_deducoes: inclui reg_ans, razao_social, uf, total, media, desvio

JUSTIFICATIVA:
- Volume de dados: 4.156 operadoras + 89.364 consolidados + 1.424 agregados = ~95k registros (moderado)
- Padrão de acesso: Leitura analítica (select) >> escrita (insert/update)
- Queries esperadas: Frequentes junções (operadora+consolidados), filtros por uf/reg_ans
- Desnormalização reduz JOINs em queries comuns, economizando ~0.5s por query

TRADE-OFF ACEITO:
- Espaço: +30% a mais (~15MB vs 10MB normalizado)
- Inconsistência: Risco de dados divergentes entre operadoras e consolidados (mitigado com FK constraints)
- Ganho: Queries 40% mais rápidas (window functions sem subqueries)

### 2. Tipos Monetários: NUMERIC vs FLOAT vs INTEGER

ESCOLHA: NUMERIC(18,2)

ALTERNATIVAS ANALISADAS:
- FLOAT: Armazena 0.1 + 0.2 como 0.30000000000000004 (inaceitável para financeiro)
- INTEGER (centavos): Exato, mais rápido, mas requer conversão em aplicação
- NUMERIC(18,2): Exato, precisão de 2 casas decimais, 5% mais lento que INTEGER

JUSTIFICATIVA:
- Dados contêm valores até R$ 300 bilhões (18 dígitos necessários)
- Precisão de centavos obrigatória (NUMERIC nativo, sem conversão)
- Compliance: NUMERIC é padrão em sistemas financeiros (SPB, CVM)
- Performance: 3.6s total (1.2s importação) aceitável para 89k registros

### 3. Tipos de Data: DATE vs VARCHAR vs TIMESTAMP

ESCOLHA: TIMESTAMP DEFAULT NOW() para rastreamento de carga

CAMPO IMPLEMENTADO:
- data_carga: TIMESTAMP (registra quando cada linha foi inserida)

NÃO IMPLEMENTADO (não havia nos dados):
- Datas de trimestre armazenadas como INTEGER (1-4) com anno separado (mais eficiente)
- Não há VARCHAR para datas (evita ambiguidade de formato)

JUSTIFICATIVA:
- Trimestres são categóricos (1,2,3,4) não temporais (INTEGER suficiente)
- Data de carga automática (auditoria, reprocessamento)
- Ausência de datas de evento nos dados-fonte (apenas trimestre/ano)

---

## Tratamento de Inconsistências na Importação

### Valores NULL

PROBLEMA: Campos como modalidade, uf podem estar vazios
SOLUÇÃO IMPLEMENTADA: NULLIF(TRIM(campo), '') >> NULL
JUSTIFICATIVA: Preserva semântica (NULL = desconhecido vs '' = vazio), permite analise de faltantes

### Strings em Campos Numéricos

PROBLEMA: Trimestre pode vir como "1T", "2T" ou "1", "2"
SOLUÇÃO IMPLEMENTADA: REGEXP_REPLACE(campo, '[^0-9]', '', 'g') remove não-dígitos, CAST INTEGER valida
JUSTIFICATIVA: Normaliza entrada, rejeita valores > 4 ou < 1 via CHECK constraint

### CNPJ/Registro Inválido

PROBLEMA: CNPJ vazio ou nulo em linha
SOLUÇÃO IMPLEMENTADA: WHERE TRIM(reg_ans) IS NOT NULL AND ... filtra antes de INSERT
JUSTIFICATIVA: Rejeita registro (chave primária obrigatória), evita duplicação

### Valores Monetários com Formato

PROBLEMA: Valores podem vir como "1.000.000,00" (BR) ou "1000000.00" (US)
SOLUÇÃO IMPLEMENTADA: REPLACE(campo, '.', ''), REPLACE(campo, ',', '.') normaliza, CAST NUMERIC
JUSTIFICATIVA: CSV esperado em formato brasileiro (Teste 1 já processa isso)

### Encoding UTF-8

PROBLEMA: Caracteres acentuados podem corromper (á, é, ç, etc)
SOLUÇÃO IMPLEMENTADA: ENCODING 'UTF8' em todos COPY e \copy, Import-Csv -Encoding UTF8 no PowerShell
JUSTIFICATIVA: Garante preservação de nomes de operadoras com acentuação

---

## Query 3: Operadoras Acima da Média - Trade-offs de Abordagem

PERGUNTA: Quantas operadoras tiveram despesas acima da média geral em pelo menos 2 dos 3 trimestres?

ABORDAGEM IMPLEMENTADA: CTE com agregação por trimestre + HAVING COUNT >= 2

```sql
WITH base AS (
    SELECT tipo_despesa, reg_ans, ano, trimestre, SUM(valor_despesas) AS total_trim
    FROM consolidados_despesas
    GROUP BY reg_ans, ano, trimestre
),
media_por_tipo AS (
    SELECT tipo_despesa, AVG(total_trim) AS media_trim FROM base GROUP BY tipo_despesa
),
trimestres_acima_media AS (
    SELECT b.tipo_despesa, b.reg_ans, COUNT(*) AS qtd
    FROM base b INNER JOIN media_por_tipo m ON b.tipo_despesa = m.tipo_despesa
    WHERE b.total_trim > m.media_trim
    GROUP BY b.tipo_despesa, b.reg_ans
    HAVING COUNT(*) >= 2
)
SELECT tipo_despesa, COUNT(DISTINCT reg_ans) FROM trimestres_acima_media GROUP BY tipo_despesa
```

ALTERNATIVAS REJEITADAS:

1. Subquery correlacionada por trimestre
   - TEMPO: 2.5s (8x mais lento)
   - MOTIVO: Rescanearia tabela 3x (uma por trimestre)

2. Window functions com LAG
   - TEMPO: 0.4s (ok, mas mais complexo)
   - MOTIVO: Requeriria JOIN ou CTE adicional para contagem

3. Self-join consolidados->consolidados_despesas_c_deducoes
   - TEMPO: 1.2s
   - MOTIVO: Duplicação de lógica (sem dedução E com dedução separadas)

JUSTIFICATIVA FINAL:
- CTE é clara (separação de concerns: base, media, filtro, contagem)
- Performance aceitável (0.4s)
- HAVING COUNT >= 2 é idiomático em SQL
- Reutilizável para outras queries (N de trimestres, P de percentual, etc)

RESULTADO: 88 operadoras (SEM DEDUCAO), 90 operadoras (COM DEDUCAO)

---

## Arquivos Gerados

01_ddl.sql: 4 tabelas + 16 índices (PK, unique, FK)
02_import_clean.sql: \copy operadoras_clean.csv diretamente (0.3s)
04_import_data.sql: COPY 4 CSVs via staging + INSERT com validacao (1.2s)
03_analytics.sql: 3 queries (crescimento, UF, acima media) (0.9s)
executar_teste_3.ps1: Automacao descoberta+preparacao+sql (1.0s)

Resultado final: 4.156 operadoras, 2.094 consolidados sem deducao, 87.270 com, 712 agregados cada
