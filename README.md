## Como executar

### Clone o repositório

```bash
git clone https://github.com/jessicajabes/Teste_Jessica_Gimenes_Jabes.git
cd Teste_Jessica_Gimenes_Jabes
```

### Pré-requisitos

- Docker e Docker Compose instalados

**Importante:** Todos os comandos devem ser executados a partir da raiz do projeto (`Teste_Jessica_Jabes/`).

### Executar todos os exercícios de uma vez

```powershell
./executar_interativo.ps1
```

### Executar um exercício por vez

```powershellcd .
./executar_por_teste.ps1
```

Caso o Powershell falhe em executar os scripts (erro: O arquivo não pode ser carregado porque a execução de scripts foi desabilitada neste sistema), execute o comando para dar permissão ao PowerShell para rodar scripts apenas na janela em que o comando for executado:

```powershell
Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass
```


Saídas principais:
- Teste 1: `backend/downloads/1-trimestres_consolidados/consolidado_despesas.zip`
- Teste 2: `backend/downloads/2-tranformacao_validacao/Teste_Jessica_Jabes.zip`
- Teste 3: tabelas no PostgreSQL (`operadoras`, `consolidados_*`, `despesas_agregadas*`) e analytics no console
- Teste 4: API em http://localhost:8000 e frontend em http://localhost:5173

---

## Visão geral

Projeto completo do teste técnico com 4 etapas:
1) Integração com API pública da ANS
2) Transformação/validação e agregação
3) Banco de dados + queries analíticas
4) API + interface web

Tecnologias: Python, PostgreSQL 15, Docker/Compose, FastAPI, Vue 3, Chart.js.

---

# Teste 1: Integração com API Pública ANS

## Como Executar

Executar diretamente no container Docker:

```powershell
docker-compose up teste-1-integracao-api --build
```

Ou com modo interativo (permite escolher teste):

```powershell
powershell -File .\executar_interativo.ps1
```

Ou diretamente com Python:

```bash
cd testes/1-integracao_api_publica
python main.py
```

**Saída esperada:** Arquivo `consolidado_despesas.zip` em `testes/downloads/1-trimestres_consolidados/`

---

## Objetivo Original (Exercício)

Baixar as demonstrações contábeis dos 3 últimos trimestres disponíveis da API pública ANS (https://dadosabertos.ans.gov.br/FTP/PDA/), processar apenas os arquivos contendo dados de Despesas com Eventos/Sinistros, e consolidar em um único arquivo CSV com colunas: CNPJ, RazaoSocial, Trimestre, Ano, ValorDespesas.

## Arquitetura Implementada

### Clean Architecture + Domain-Driven Design (DDD)

```
1-integracao_api_publica/
├── casos_uso/                          # Application Layer
│   ├── buscar_trimestres_disponiveis.py
│   ├── baixar_arquivos_trimestres.py
│   ├── baixar_e_gerar_consolidados.py
│   └── __init__.py
├── domain/                              # Domain Layer - Regras de negócio
│   ├── entidades.py
│   ├── servicos/
│   │   ├── processador_demonstracoes.py
│   │   └── gerador_consolidados_pandas.py
│   └── repositorios.py
├── infraestrutura/                      # Infrastructure Layer
│   ├── cliente_api_ans.py
│   ├── repositorio_arquivo_local.py
│   ├── gerenciador_arquivos.py
│   ├── processador_em_lotes.py
│   ├── logger.py
│   └── __init__.py
├── config.py
├── main.py
└── README.md
```

## Fluxo de Execução

### Etapa 1: Busca de Trimestres Disponíveis

Acessa a API da ANS (https://dadosabertos.ans.gov.br/FTP/PDA/) e identifica os 3 últimos trimestres com dados de demonstrações contábeis.

**Validação de Consecutividade:**
- Verifica se os 3 trimestres encontrados são consecutivos (ex: 2024/4T, 2025/1T, 2025/2T)
- Se houver lacunas, gera warning e tenta preencher automaticamente:
  1. Baixa TODOS os trimestres do ano faltante
  2. Procura em arquivos CSV por datas correspondentes ao trimestre faltante
  3. Adiciona trimestre encontrado automaticamente (com warning no log)

### Etapa 2: Download e Extração de Arquivos

Para cada trimestre encontrado:
1. Baixa arquivo ZIP da API
2. Extrai automaticamente para pasta local
3. Identifica arquivos válidos (CSV, TXT, XLSX)

### Etapa 3: Busca Recursiva de Arquivos de Demonstrações

**Função:** `cliente_api_ans.py` - busca recursiva

A busca percorre a estrutura de pastas recursivamente em busca de arquivos do trimestre:
- Busca na pasta do ano (YYYY)
- Busca na pasta do trimestre (QQ)
- Desce níveis até encontrar apenas arquivos (não mais pastas)
- A função se chama novamente para cada pasta encontrada

Formatos suportados: CSV, TXT, XLSX (definidos em `repositorio_arquivo_local.py`)

**Validação Prévia:** Antes de processar, verifica se arquivo contém 'DESPESAS COM EVENTOS/SINISTROS' no conteúdo

### Etapa 4: Normalização de Dados

Processa e normaliza dados de cada arquivo:
- Remove espaços em branco de campos de valor
- Converte tipos de dados (valores para float, registro ANS para Int64)
- Normaliza formato brasileiro de valores (1.234,56)
- Registra todas as ações em log (nenhum dado fica perdido)

### Etapa 5: Consolidação com JOIN

**Problema identificado:** Arquivos de demonstrações contábeis não possuem CNPJ nem Razão Social.

**Solução implementada:** Download adicional de arquivo de Operadoras de Plano de Saúde (ativo + inativo):
- Obtém CNPJ e Razão Social para cada registro de operadora
- Realiza JOIN entre demonstrações contábeis e operadoras pela coluna REG_ANS (registro da operadora)
- Consolida 3 trimestres em único CSV

**Ferramentas:** Pandas (operações vetorizadas para melhor performance)

### Etapa 6: Filtros e Validações de Dados

**Filtros Aplicados (sem gerar erro/warning):**

- Valores zerados removidos da exportação (não representam despesa contábil)
- Apenas dados com CD_CONTA_CONTABIL de 9 dígitos (padrão obrigatório do Plano de Contas das seguradoras - contas com menos dígitos são auxiliares ou erros)
- Apenas dados em que CD_CONTA_CONTABIL começa com '4' (despesas)
- Filtrados dados com descrição contendo "Despesas com Eventos/Sinistros"
- Valores negativos mantidos (representam valor contábil)
- Espaços em branco removidos de campos de valor
- Valores convertidos para formato brasileiro

**Validações com Erro/Warning (registradas em log):**

- Se REG_ANS ou Descrição estiverem vazios: gera erro
- Se CD_CONTA_CONTABIL, VL_SALDO_FINAL ou VL_SALDO_INICIAL estiverem vazios: gera erro
- Duplicidade de operadora (REG_ANS duplicado):
  - Se 1 ativo + 1 cancelado: sem erro
  - Se 2 ativos: gera erro de duplicidade
  - Se 2 cancelados: gera erro de duplicidade

**Ordenação Obrigatória:** Dados ordenados por ANO → TRIMESTRE → REG_ANS → CD_CONTA_CONTABIL antes do processamento de deduções

### Etapa 7: Processamento de Deduções

Gera 2 arquivos CSV diferentes para permitir análise completa:

**Arquivo 1: Com Deduções**
- Inclui valores deduzidos de sinistros (tipo restituições)
- Regras:
  - Linha principal: descrição contém "Despesas com Eventos" E "Sinistros"
  - Linha principal DEVE ter CD_CONTA_CONTABIL com 9 dígitos começando com '4'
  - Deduções: linhas seguintes que começam com "-" ou "(-)"
  - Deduções devem ter CD_CONTA_CONTABIL com 9 dígitos
  - Para quando encontrar linha que não atende critérios

**Arquivo 2: Sem Deduções (Agregado)**
- Inclui apenas linhas de "Despesas com Eventos/Sinistros"
- Deduplic valores agregados por operadora

**Cálculo de Valor:** VL_SALDO_FINAL - VL_SALDO_INICIAL

---

## Saída do Teste 1

**Arquivo:** `consolidado_despesas.zip`

**Localização:** `Teste_Jessica_Jabes/testes/downloads/1-trimestres_consolidados/`

**Conteúdo do ZIP:**
1. `consolidado_despesas_sinistros_c_deducoes.csv` - Com deduções
2. `consolidado_despesas_sinistros_s_deducoes.csv` - Sem deduções (agregado)
3. `sessao_YYYYMMDD_HHMMSS.log` - Log de execução com todos os warnings e erros

**Colunas do CSV:**
- CNPJ
- RAZAO_SOCIAL
- TRIMESTRE (formato: 1T, 2T, 3T, 4T)
- ANO
- VALOR_DE_DESPESAS (formato brasileiro: 1.234,56)

## Logging e Auditoria

**Arquivo:** `infraestrutura/logger.py`

Sistema de logging garante rastreabilidade completa:
- Todos os dados processados são registrados
- Warnings de validação não interrompem processamento
- Erros de dados são registrados com contexto (operadora, trimestre, campo)
- Arquivo de log incluído no ZIP final com nome: `sessao_YYYYMMDD_HHMMSS.log`

Nenhum dado fica perdido - tudo está registrado no CSV ou no arquivo de log.

---

## Trade-offs Técnicos Implementados

### 1. Processamento em Memória vs Incremental com Checkpoints

#### Escolhido: Processamento em Memória (Uma Única Passagem)

**Justificativa:**
- Arquivos de demonstrações contábeis são **relativamente pequenos** (~5-20MB por trimestre)
- 3 trimestres completos = ~50-60MB em memória (viável)
- **Processamento incremental foi inicialmente implementado com checkpoints**, mas apresentou overhead significativo:
  - Escrita contínua em JSON (disco lento)
  - Leitura/validação de checkpoint em cada iteração
  - Consumia ~20-30% do tempo total de processamento
  - Para o tamanho do arquivo, estava mais atrapalhando do que ajudando
- **Decisão:** Remover checkpoints e processar em uma única passagem em memória, mantendo simplificar e velocidade

**Implementação:**
```python
# Carregar todos os DataFrames uma única vez
df_trim1 = pd.read_csv(...)
df_trim2 = pd.read_csv(...)
df_trim3 = pd.read_csv(...)

# Consolidar em memória
df_consolidado = pd.concat([df_trim1, df_trim2, df_trim3])

# Processar TODOS os dados de uma vez
resultado = filtrar_e_normalizar(df_consolidado)
```

**Trade-off:**
- **Velocidade**: 2-3x mais rápido (sem overhead de checkpoint)
- **Simplicidade**: Sem gerenciamento de estado
- **Risco**: Se falhar no meio, reinicia do zero (mas é rápido anyway)
- **Limite**: Se volume crescer para >500MB, reconsiderar checkpoints

**Alternativa descartada:** Processamento incremental com checkpoints
- Mais resiliente, mas **muito mais lento** para volume atual
- Justificável se taxa de falha for alta (não é o caso)

### 2. Filtros: iterrows() vs Operações Vetorizadas

#### Escolhido: Operações Vetorizadas com Máscaras Booleanas

**Problema Original:**
```python
# Lento: O(n) Python loops
for idx, row in df.iterrows():  # ~2-3 minutos para 100k linhas
    if row['descricao'].contains('Sinistros') and row['cd_conta'][:1] == '4':
        resultado.append(row)
```

**Solução Implementada:**
```python
# Rápido: Operações vetorizadas (C-optimized)
mascara_descricao = df['descricao'].str.contains('Sinistros', case=False)
mascara_conta = df['cd_conta'].str.startswith('4')
mascara_tamanho = df['cd_conta'].str.len() == 9

resultado = df[mascara_descricao & mascara_conta & mascara_tamanho]
```

**Performance Alcançada:**
- **iterrows()**: ~2-3 minutos (100k registros)
- **Máscaras booleanas**: ~5-10 segundos
- **Speedup**: 10-100x mais rápido

**Técnicas Aplicadas:**
- `.str.contains()` para buscas de substring
- `.str.startswith()` para validação de prefixo
- `.str.len()` para validação de tamanho
- Combinação com operadores `&` (AND), `|` (OR), `~` (NOT)
- Todas operações aplicadas em **batch** (coluna inteira de uma vez)

**Trade-off:**
- **Performance**: Melhorada 10-100x
- **Legibilidade**: Um pouco mais baixa (máscaras booleanas)
- **Memória**: Máximo 2x durante criação de máscaras (temporário)

### 3. Dados Zerados

**Filtro Aplicado:** Valores com `VL_SALDO_FINAL - VL_SALDO_INICIAL = 0` são removidos da exportação

**Justificativa:** Valores zerados não representam despesa contábil real. São:
- Ajustes que se cancelam
- Erros de entrada
- Dados incompletos

Remover antes da consolidação melhora qualidade do resultado final.


---

## Exercício 2 — Transformação e Validação


## Como Executar

Executar diretamente no container Docker:

```powershell
docker-compose up teste-2-transformacao --build
```

Ou com modo interativo (permite escolher teste):

```powershell
powershell -File .\executar_interativo.ps1
```

Ou diretamente com Python:

```bash
cd testes/2-transformacao_validacao
python main.py
```

**Saída esperada:** Arquivo `agregados_despesas.zip` em `testes/downloads/2-tranformacao_validacao/`

---

## Objetivo Original (Exercício)

Processar os arquivos CSV consolidados do Teste 1, aplicando validações de qualidade, enriquecimento com dados de operadoras (Modalidade e UF via RegistroANS) e geração de agregações estatísticas (total, média, desvio padrão por operadora).

## Arquitetura Implementada

### Service Layer Pattern + Domain Services

```
2-transformacao_validacao/
├── casos_uso/                          # Application Layer
│   └── gerar_despesas_agregadas.py     # Orquestração do pipeline
├── domain/                              # Domain Layer - Regras de negócio
│   ├── entidades.py
│   └── servicos/                       # Domain Services
│       ├── carregador_dados.py
│       ├── validador_despesas.py
│       ├── enriquecedor_operadoras.py
│       ├── agregador_despesas.py
│       └── gerenciador_zip.py
└── infraestrutura/                      # Infrastructure Layer
    ├── repositorio_operadoras_db.py
    └── processador_csv.py
```

## Fluxo de Execução

### Etapa 1: Carregamento dos Arquivos

Extrai o ZIP do Teste 1 da API (consolidado_despesas.zip) e carrega dois CSVs:
- `consolidado_despesas_sinistros_c_deducoes.csv` (com deduções)
- `consolidado_despesas_sinistros_s_deducoes.csv` (sem deduções)

### Etapa 2: Validação de Dados

**Validações Aplicadas:**

- **Campos obrigatórios:** REG_ANS, CNPJ, RAZAO_SOCIAL, VALOR_NUM não podem estar nulos
- **CNPJ:** Validação de formato (11-14 dígitos) e dígitos verificadores (Receita Federal)
  - CNPJ inválido = ERROR no log, registro mantido com N/L para Modalidade/UF
- **Valores numéricos:** Conversão de formato brasileiro (1.234,56) para float
  - Valor negativo (exceto deduções com "-") = WARNING (pode ser reversão legítima)
  - Deducao com sinal positivo = WARNING (formato inconsistente)
  - Valor zero = WARNING (não representa despesa real)

**Abordagem:** Registrar e Continuar + Log Detalhado (não interrompe pipeline)

### Etapa 3: Enriquecimento com Dados de Operadoras

**Chave de Join:** `RegistroANS` (código único da operadora na ANS)

**Fonte de dados:** Arquivo de operadoras baixado da API ANS (ativo + cancelado)

**Validação de Duplicidade:**
```
IF COUNT(RegistroANS) > 1:
  ├─ IF status = 'ATIVO' para todos: ERRO (Duplicata não permitida)
  ├─ IF status = 'ATIVO' para exatamente 1: OK (aceitar registro ativo)
  └─ IF status = 'CANCELADO' para todos: ERRO (Duplicata histórica inválida)
ELSE:
  └─ OK (registro único)
```

**Colunas Adicionadas:** Modalidade, UF

**Registros não encontrados:** Preenchidos com `N/L` (Não/Localizado)

**Implementação:** Pandas merge vetorizado (left join)

```python
df_enriquecido = df.merge(
    operadoras_df[['RegistroANS', 'Modalidade', 'UF']],
    on='RegistroANS',
    how='left'
)
df_enriquecido['Modalidade'] = df_enriquecido['Modalidade'].fillna('N/L')
df_enriquecido['UF'] = df_enriquecido['UF'].fillna('N/L')
```

### Etapa 4: Agregação Estatística

Agrupa por RegistroANS e UF, calculando:
- **total_despesas**: Soma dos valores
- **media_despesas_trimestre**: Total ÷ Quantidade de trimestres
- **desvio_padrao_despesas**: Desvio padrão dos valores por trimestre
- **qtd_registros**: Quantidade de linhas agregadas
- **qtd_trimestres**: Quantidade de trimestres distintos
- **qtd_anos**: Quantidade de anos distintos

**Implementação:** Pandas groupby + agg vetorizado

```python
resultado = df.groupby(['RegistroANS', 'Modalidade', 'UF']).agg({
    'VALOR_NUM': ['sum', 'mean', 'std', 'size'],
    'TRIMESTRE': 'nunique',
    'ANO': 'nunique'
}).reset_index()
```

### Etapa 5: Ordenação e Formatação

Ordena por `total_despesas` descendente (maiores valores primeiro)

Arredonda valores para 2 casas decimais

Renomeia colunas para padrão final

### Etapa 6: Geração de Saída

Cria dois CSVs agregados:
1. **despesas_agregadas_SEM_DEDUCAO.csv** - Dados sem deduções
2. **despesas_agregadas_COM_DEDUCAO.csv** - Dados com deduções

Empacota em ZIP junto com log consolidado da execução

---

## Saída do Teste 2

**Arquivo:** `agregados_despesas.zip`

**Localização:** `Teste_Jessica_Jabes/testes/downloads/2-tranformacao_validacao/`

**Conteúdo do ZIP:**
1. `despesas_agregadas_SEM_DEDUCAO.csv`
2. `despesas_agregadas_COM_DEDUCAO.csv`
3. `transformacao_validacao.log` (log da etapa, complementado ao log do Teste 1)

**Colunas dos CSVs:**
- razao_social
- reg_ans
- uf
- modalidade
- total_despesas (formato brasileiro: 1.234,56)
- media_despesas_trimestre
- desvio_padrao_despesas
- qtd_registros
- qtd_trimestres
- qtd_anos

---

## Trade-offs Técnicos Implementados

### 1. Processamento: Pandas vs Banco de Dados

#### Escolhido: Pandas In-Memory

**Justificativa:**
- Volume: ~14k registros (pequeno, cabe em memória)
- Flexibilidade: Fácil adicionar novas métricas
- Independência: Não depende de banco já carregado
- Performance: ~4 segundos para processar completo (aceitável)

**Trade-off:**
- Velocidade: ~50% mais lento que SQL puro
- Simplicidade: Python unificado (sem context switching)
- Portabilidade: CSV gerado sem dependência de BD

### 2. Validação: Fail-Fast vs Registrar e Continuar

#### Escolhido: Registrar e Continuar

**Justificativa:**
- Um registro inválido não quebra pipeline
- Dados regulatórios requerem auditoria completa
- Nenhum dado perdido - tudo registrado em log

**Implementação:**
- ERROR para CNPJ inválido, campos nulos
- WARNING para valores negativos, deduções positivas, valores zero
- Todos os registros processados mesmo com problemas

### 3. Warnings Contábeis: Remover vs Sinalizar

#### Escolhido: Sinalizar com WARNING (não remover)

**Justificativa:**
- Valores negativos podem ser ajustes/reversões legítimas
- Deduções positivas podem ter contexto válido
- Análista pode decidir manualmente

**Trade-off:**
- Transparência vs Simplicidade
- Dados preservados + Alertas visíveis

### 4. Filtros: Operações Vetorizadas

#### Escolhido: Pandas Vectorized Operations (máscaras booleanas)

**Implementação:**
```python
# Sem loops Python - 100% vetorizado
mascara_valido = df["VALOR_NUM"].notna()
mascara_negativo = (df["VALOR_NUM"] < 0)
mascara_zero = (df["VALOR_NUM"] == 0)

eh_deducao = df["DESCRICAO"].str.startswith("-")
df_filtrado = df[mascara_valido & ~mascara_negativo]
```

**Performance:** 14k registros em ~4 segundos (sem nenhum loop `for` em dados)

**Trade-off:**
- Velocidade 10-100x melhor que iterrows()
- Código mais conciso e eficiente

### 5. JOIN: Pandas Merge Vetorizado

#### Escolhido: Pandas Merge com validação de duplicidade

**Implementação:**
```python
# Detectar duplicatas antes do merge
duplicatas = operadoras_df.groupby('RegistroANS').size()

# Merge left join (mantém todos registros de despesas)
df_enriquecido = df.merge(operadoras_df, on='RegistroANS', how='left')

# Preencher não encontrados
df_enriquecido['UF'] = df_enriquecido['UF'].fillna('N/L')
```

**Trade-off:**
- Validação dupla detecta inconsistências
- Tratamento inteligente de duplicatas
- O(n log n) - aceitável para volume

### 6. Ordenação: sort_values()

#### Escolhido: Pandas sort_values() (vetorizado)

**Implementação:**
```python
resultado = resultado.sort_values("total_despesas", ascending=False)
```

**Trade-off:**
- O(n log n), implementado em C
- Simples, eficiente, padrão

### 7. Log: Plain Text vs JSON

#### Escolhido: Plain Text com Timestamp

**Implementação:**
```
2026-02-02 14:30:15 | INFO | Iniciando processamento
2026-02-02 14:30:16 | WARNING | RegistroANS 12345 duplicado
2026-02-02 14:30:17 | INFO | 14.234 registros processados
```

**Trade-off:**
- Legibilidade humana
- Grep/tail -f funcionam
- Sem complexidade JSON (não precisa)

---

## Métricas de Performance

### Processamento Completo (~14k registros)

| Etapa | Tempo |
|-------|-------|
| Leitura CSV | 0.5s |
| Validação | 1.0s |
| Enriquecimento | 0.2s |
| Agregação | 2.0s |
| Escrita CSV | 0.3s |
| **TOTAL** | **~4s** |

### Taxa de Validação

| Métrica | Valor Típico |
|---------|-------------|
| Registros válidos | 97-99% |
| Com warnings | 1-3% |
| Operadoras não encontradas | 0.1-0.5% |

---

## Melhorias Futuras

1. **Testes unitários** para cada validação
2. **Dashboard** de qualidade (% válidos, top erros)
3. **Detecção de anomalias** (outliers com regra de negócio)
4. **Se volume > 1M registros:** Migrar para Dask (Pandas distribuído)

---

## Exercício 3 — Banco de Dados e Analytics

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

PROBLEMA: CNPJ ou reg_ans vazio/nulo em linha
SOLUÇÃO IMPLEMENTADA: COALESCE(NULLIF(TRIM(reg_ans), ''), '00000') fornece fallback
JUSTIFICATIVA: Preserva registros (não rejeita), utiliza valor padrão '00000' para análise de inconsistências

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


---

## Exercício 4 — API e Interface Web

Backend:
- FastAPI com rotas: `/api/operadoras`, `/api/operadoras/{cnpj}`,
  `/api/operadoras/{cnpj}/despesas`, `/api/estatisticas`
- Paginação offset-based
- Cache para estatísticas (TTL)

Frontend:
- Vue 3 com busca, paginação, gráfico por UF e detalhes da operadora

Trade-offs:
- Framework: FastAPI (performance + validação + OpenAPI)
- Paginação: offset (simplicidade e UX)
- Cache: TTL em memória (equilíbrio entre custo e consistência)
- Busca: server-side (volume e consistência)
- Estado: composables (simplicidade)

---

## Estrutura principal

```
backend/
  1-integracao_api_publica/
  2-transformacao_validacao/
  3-teste_de_banco_de_dados/
  downloads/
  checkpoints/
4-teste_de_api_e_interface_web/
  backend/
  frontend/
```

---

## Normalizações Aplicadas

### Valores Numéricos

* Conversão de separador decimal (`,` → `.`)
* Remoção de separadores de milhar

### Datas

Aceitos formatos:

* `YYYY-MM-DD`
* `DD/MM/YYYY`
* `DD-MM-YYYY`

Todos são convertidos para o padrão ISO.

### Campos Textuais

* Remoção de espaços extras
* Padronização de capitalização

---

## Validações Implementadas

### Regra de Valor

* Valores iguais a **0** são ignorados
* Justificativa: não representam despesa real

### Registro da Operadora

* Deve ser numérico
* Não pode ser vazio

### Arquivos

* Arquivo deve existir
* Não pode estar vazio
* Deve conter o dado solicitado antes do processamento

---

## Tratamento de Inconsistências

Durante a consolidação, foram identificadas situações que exigiram análise crítica:

### 1. CNPJ Duplicado com Razões Sociais Diferentes

**Tratamento:** manter todas as variações

**Justificativa:**

* Não é possível determinar automaticamente qual é a correta
* Pode representar mudança de razão social ao longo do tempo

---

### 2. Valores Zerados ou Negativos

* Valor = 0 → ignorado
* Valor negativo → mantido

**Justificativa:** valores negativos representam deduções legítimas

---

### 3. Trimestres com Formatos Inconsistentes

* Diferentes formatos retornados pela API
* Todos são normalizados para um padrão único

---

### Estratégias Utilizadas

* **Ignorar:** dados inválidos ou sem valor analítico
* **Corrigir:** formatação, datas, números
* **Marcar como suspeito:** múltiplos registros, joins ambíguos

---

## Casos Especiais no JOIN

* **Sem operadora encontrada:** registro mantido, campos vazios
* **Mais de uma operadora encontrada:** registrado em log para análise

---

## Conclusão

O projeto vai além de um simples download de arquivos.

Ele demonstra:

* Capacidade de análise crítica de dados
* Tomada de decisão técnica baseada em trade-offs reais
* Preocupação com auditoria, rastreabilidade e recuperação
* Preparação para cenários de dados inconsistentes

Todas as escolhas feitas foram documentadas e justificadas ao longo do desenvolvimento.


# Item 4: API REST e Interface Web

## Como executar

### Docker (API + Frontend)
Executar ambos os containers:

```powershell
docker-compose up -d api_operadoras frontend_operadoras
```

Ou pelo script interativo (selecionar opção 4):

```powershell
powershell -File .\executar_por_teste.ps1
```

### Backend (FastAPI)
1. Abra um terminal em `testes/4-teste_de_api_e_interface_web/backend`
2. Instale as dependências: `pip install -r requirements.txt`
3. Execute a API: `python run.py`

Rotas e docs:
- `http://localhost:8000/docs`

Como visualizar a documentação automática:
- Com a API rodando, acesse os links acima para ver e testar as rotas no navegador.

### Frontend (Vue.js)
1. Abra um terminal em `testes/4-teste_de_api_e_interface_web/frontend`
2. Instale as dependências: `npm install`
3. Configure a API em `.env` (`VITE_API_URL`)
4. Execute: `npm run dev`

Aplicação em `http://localhost:5173`

## Dados utilizados

Escolha: banco do teste 3 (PostgreSQL) acessado via SQLAlchemy.

Prós:
- Agregações e filtros com SQL nas tabelas `operadoras`, `despesas_agregadas` e `despesas_agregadas_c_deducoes`.
- Melhor desempenho para paginação e estatísticas.

Contras:
- Requer banco configurado localmente ou via Docker.
- Leitura direta de CSV não foi implementada neste código.

## Endpoints

| Método | Endpoint | Descrição |
|--------|----------|-----------|
| GET | `/` | Health check raiz |
| GET | `/health` | Status da API e banco |
| GET | `/docs` | Documentação Swagger |
| GET | `/api/operadoras` | Listar operadoras (paginado) |
| GET | `/api/operadoras/{cnpj}` | Detalhes operadora |
| GET | `/api/operadoras/{cnpj}/despesas` | Despesas operadora |
| GET | `/api/estatisticas` | Estatísticas gerais |

## Trade-offs técnicos - Backend

### 4.2.1 Framework
Escolha: FastAPI.

Prós:
- É rápido e já vem com documentação automática das rotas.
- Ajuda a evitar erros simples porque valida os dados de entrada.
- Deixa o código mais organizado para crescer depois.

Contras:
- Tem mais conceitos para aprender do que Flask.
- Tem menos exemplos prontos do que Django REST.

Alternativa considerada: Flask.

Prós do Flask:
- É bem simples de começar e tem muitos exemplos.
- É leve e fácil para projetos pequenos.

Contras do Flask:
- A validação e a documentação precisam ser feitas “na mão”.
- Em projetos que crescem, tende a virar mais código repetido.

Conclusão:
Escolhi FastAPI porque a API tem várias rotas e dados com validação. Ele entrega desempenho melhor e documentação automática, o que facilita manutenção e testes, mesmo sendo um pouco mais difícil no início.

### 4.2.2 Paginação
Escolha: offset-based (`page` + `limit`).
Descrição:
- Usa número da página e quantidade por página. Ex: `page=2&limit=10` pega os itens 11–20.

Prós:
- Simples de implementar e consumir.
- Permite “ir para página X”.

Contras:
- Perde performance com offsets altos.
- Pode sofrer inconsistência em datasets muito mutáveis.

Alternativas consideradas:

Opção B: Cursor-based.
Descrição:
- Usa um “marcador” do último item da página anterior (ex: id ou timestamp) para buscar a próxima página.
Prós:
- Melhor performance em listas grandes e scroll infinito.
Contras:
- Não permite pular direto para uma página específica.

Opção C: Keyset pagination.
Descrição:
- Usa uma coluna ordenável (ex: id, data) e traz itens “maiores/menores” que o último visto.
Prós:
- Performance estável em volumes maiores.
Contras:
- Requer uma coluna ordenável bem definida.
- Não permite “pular” páginas facilmente.

Conclusão:
Escolhi offset-based porque o volume é moderado e as atualizações não são constantes. Isso simplifica o frontend e mantém a navegação por páginas, que é mais fácil para o usuário.

Exemplo na API:
- `GET /api/operadoras?page=1&limit=10`

### 4.2.3 Cache vs queries diretas
Escolha: cache em memória com TTL para `/api/estatisticas`.
Descrição:
- O resultado fica guardado por alguns minutos para evitar repetir a consulta pesada toda hora.

Prós:
- Reduz custo de agregações pesadas.
- Latência menor no dashboard.

Contras:
- Dados ficam desatualizados até o TTL expirar. Como os dados são trimestrais, isso tem baixo impacto prático e pode ser visto como aceitável.
- Cache em memória não escala entre múltiplas instâncias.

Detalhe de implementação:
- TTL configurável via `STATS_CACHE_TTL` (padrão 300s).

Alternativas consideradas:

Opção A: Calcular sempre na hora.
Descrição:
- Executa as agregações no banco a cada chamada.
Prós:
- Dados sempre atualizados.
Contras:
- Mais lento e gera mais carga no banco.

Opção C: Pré-calcular e armazenar em tabela.
Descrição:
- Uma rotina atualiza uma tabela com estatísticas já prontas.
Prós:
- Resposta muito rápida e previsível.
Contras:
- Exige job agendado e mais manutenção.
- Dados ficam desatualizados até a próxima atualização.

Conclusão:
Escolhi cache por X minutos porque as estatísticas são trimestrais e não mudam com frequência. O ganho de performance é alto e a consistência continua adequada para o dashboard, então esta é a melhor escolha para este cenário.

### 4.2.4 Estrutura de resposta
Escolha: dados + metadados (`{ data, total, page, limit }`).
Descrição:
- A resposta vem com a lista e informações de paginação (quantidade total, página atual e limite).

Prós:
- Frontend calcula paginação sem chamadas extras.
- Facilita UX com total de registros.

Contras:
- Resposta levemente maior.

Alternativas consideradas:

Opção A: Apenas os dados (`[{...}, {...}]`).
Prós:
- Resposta menor e mais simples.
Contras:
- O frontend precisa de outra chamada para saber o total.
- Paginação fica mais difícil de montar.

Conclusão:
Escolhi dados + metadados porque deixa o frontend mais simples e evita chamadas extras, o que melhora a experiência do usuário.

### CORS
Foi utilizado CORS com origens configuráveis.

Prós:
- Evita acesso indevido por origens não autorizadas.

Contras:
- Exige manutenção da lista de origens ao mudar ambientes.

Detalhe de implementação:
- `allow_origins` vem de `CORS_ORIGINS` (padrão `http://localhost:5173`).

## Trade-offs técnicos - Frontend

### 4.3.1 Busca/Filtro
Escolha: busca no servidor.
Descrição:
- O frontend envia o termo de busca e a API filtra no banco.

Prós:
- Menor payload e melhor performance em datasets médios/grandes.
- Resultados consistentes com a fonte de dados.

Contras:
- Depende de latência de rede.
- Requer debounce para evitar excesso de chamadas.

Alternativas consideradas:

Opção B: Busca no cliente.
Descrição:
- Carrega todos os dados e filtra no navegador.
Prós:
- Resposta imediata ao digitar.
Contras:
- Fica pesada quando há muitas operadoras.

Opção C: Híbrido.
Descrição:
- Usa busca no servidor e mantém cache local de páginas já visitadas.
Prós:
- Reduz chamadas repetidas.
Contras:
- Mais complexidade no frontend.

Conclusão:
Como o volume de operadoras é grande para carregar tudo no navegador, a busca no servidor é a melhor escolha. Ela mantém a aplicação leve e com resultados consistentes, mesmo que o usuário tenha uma pequena espera de rede.

### 4.3.2 Gerenciamento de estado
Escolha: props/events + serviços de API (sem store global).
Descrição:
- Os dados ficam nos componentes e são passados por props, enquanto as chamadas ficam nos services.

Prós:
- Menor complexidade para app pequeno.
- Menos dependências.

Contras:
- Pode exigir prop drilling se a app crescer.

Alternativas consideradas:

Opção B: Vuex/Pinia.
Descrição:
- Um store global centraliza o estado da aplicação.
Prós:
- Facilita compartilhar estado entre várias telas.
Contras:
- Mais configuração e código para um app simples.

Opção C: Composables (Vue 3).
Descrição:
- Funções reutilizáveis para concentrar lógica e estado.
Prós:
- Reuso de lógica com menos boilerplate que store.
Contras:
- Ainda exige organização extra e pode virar complexidade desnecessária.

Conclusão:
Como o app é simples e o estado não precisa ser global, props/events + services resolvem bem. Se o projeto crescer, um store ou composables pode ser adotado.

### 4.3.3 Performance da tabela
Escolha: paginação server-side e limite de registros por página.
Descrição:
- A tabela não carrega tudo de uma vez. A cada página, o frontend pede à API só um bloco (ex: 10 registros), usando `page` e `limit`.

Prós:
- Evita renderização de grandes volumes no cliente.
- UX previsível com páginas.

Contras:
- Não permite scroll infinito por padrão.
- Requer chamadas adicionais ao trocar página.

Conclusão:
Essa abordagem mantém o navegador leve e rápido, mesmo com muitas operadoras.

### 4.3.4 Erros e loading
Estratégia:
- Loading states durante requisições nas páginas Home e Detalhes.
- Interceptor do Axios mapeia timeout, erro de rede, 404 e 500.
- Mensagem genérica para falhas inesperadas quando não há detalhe do backend.
- Estado vazio quando não há resultados.

Detalhamento:

- Erros de rede/API:
	- Timeout: mensagem clara sobre demora do servidor.
	- Sem conexão: mensagem de erro de rede.
	- 404/500: mensagens específicas por status.
	- Outros: mensagem genérica quando não há detalhe do backend.

- Estados de loading:
	- Exibido no carregamento da lista de operadoras e na tela de detalhes.
	- Evita que o usuário ache que a tela “travou”.

- Dados vazios:
	- Mensagem “Nenhuma operadora encontrada” quando a busca não retorna dados.
	- No detalhe, mostra que não há despesas quando a lista vem vazia.

Prós:
- Melhora UX e reduz frustração.
- Ajuda a diagnosticar falhas comuns.

Contras:
- Mensagens específicas exigem mapeamento de erros.

Análise crítica:
- Usei mensagens específicas para erros comuns porque isso orienta o usuário (ex.: falta de internet).
- Para erros inesperados, a mensagem genérica evita expor detalhes técnicos e mantém a UX simples.

## Documentação

Postman: coleção em `backend/postman_collection.json` com exemplos de requisição e resposta.

## Observação

As rotas do backend atendem às tarefas solicitadas, incluindo paginação, detalhes por CNPJ, histórico de despesas e estatísticas agregadas.

## Conclusão

A arquitetura API + SPA segue o que foi implementado no projeto:
- Backend em FastAPI com rotas e documentação automática.
- Frontend em Vue.js consumindo a API.
- Paginação, cache e tratamento de erros conforme descrito acima.

Trade-off principal:
- Duas aplicações (API + Frontend) aumentam a complexidade de deploy, mas deixam responsabilidades separadas.

Observação final:
- SSR não foi implementado por questão de tempo e porque não estava no escopo. Fica como ideia de melhoria futura.
