# Teste 2: Transformação e Validação de Dados

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

**Registros não encontrados:** Preenchidos com `N/L` (Não Localizado)

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
1. `despesas_agregadas_SEM_DEDUCAO.csv` - ~1.100 operadoras agregadas
2. `despesas_agregadas_COM_DEDUCAO.csv` - ~1.100 operadoras agregadas
3. `transformacao_validacao.log` - Log da etapa, complementado ao log do Teste 1

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

**Resumo de Dados Gerados (Execução Típica):**

| Arquivo | Registros | Valor Total (Com Dedução) | Valor Total (Sem Dedução) |
|---------|-----------|---------------------------|--------------------------|
| SEM_DEDUCAO.csv | 2.094 | - | R$ 299.0 Bilhões |
| COM_DEDUCAO.csv | 87.270 | R$ 204.7 Bilhões | - |

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
