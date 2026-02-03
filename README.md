## Como executar

Pré-requisitos: Docker e Docker Compose.

**Importante:** Todos os comandos devem ser executados a partir da raiz do projeto (`Teste_Jessica_Jabes/`).

### Executar todos os exercícios de uma vez

```powershell
./executar_interativo.ps1
```

### Executar um exercício por vez

```powershell
./executar_por_teste.ps1
```

### Executar somente o Teste 3 (import + analytics)

```powershell
./backend/3-teste_de_banco_de_dados/import_csvs.ps1
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

O que faz:
- Valida CNPJ, valores positivos e razão social
- Enriquecimento com cadastro de operadoras
- Agrega por razão social/UF
- Gera `despesas_agregadas.csv` e `despesas_agregadas_c_deducoes.csv` no ZIP final

Trade-offs:
- CNPJs inválidos: registros descartados (evita poluir agregações).
- Join com cadastro: usando banco (mais confiável para volume e consistência).
- Ordenação: feita em SQL para evitar custo em memória.

---

## Exercício 3 — Banco de Dados e Analytics

DDL e import:
- Tabelas normalizadas: `operadoras`, `consolidados_despesas`, `consolidados_despesas_c_deducoes`,
  `despesas_agregadas`, `despesas_agregadas_c_deducoes`.
- Tipos: `NUMERIC(18,2)` para valores e `INTEGER` para ano/trimestre.

Trade-offs:
- Normalização escolhida: reduz redundância, facilita manutenção e analytics.
- Tipos numéricos: `NUMERIC` para precisão de valores monetários.

Queries analíticas (03_analytics.sql):
- Top 5 crescimento percentual (primeiro vs último trimestre disponível)
- Top 5 UF por total e média por operadora
- Operadoras acima da média em 2+ trimestres (CTEs pela legibilidade)

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
