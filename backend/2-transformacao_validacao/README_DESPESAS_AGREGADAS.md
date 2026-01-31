# Despesas Agregadas - Análise por Operadora e UF

## 📊 Visão Geral

Este módulo gera análises agregadas das despesas, agrupando dados por Razão Social (operadora) e UF, com cálculos estatísticos detalhados.

## 🎯 Funcionalidades

### 1. Processamento de Dois Tipos de Despesas

O sistema processa separadamente:
- **Despesas com Sinistros** (`consolidado_despesas_sinistros.csv`)
- **Todas as Despesas** (`consolidado_todas_despesas.csv`)

### 2. Agregações Geradas

Para cada tipo de despesa, o sistema calcula:

| Métrica | Descrição |
|---------|-----------|
| **Total de Despesas** | Soma total das despesas por operadora e UF |
| **Média de Despesas por Trimestre** | Média dos valores de despesas por trimestre |
| **Desvio Padrão das Despesas** | Dispersão dos valores de despesas |
| **Quantidade de Registros** | Total de registros agrupados |
| **Quantidade de Trimestres** | Número de trimestres distintos |
| **Quantidade de Anos** | Número de anos distintos |

### 3. Ordenação

Os resultados são ordenados do **maior para o menor** total de despesas, facilitando a identificação das operadoras com maiores gastos.

## 📁 Arquivo de Saída

### Nome: `despesas_agregadas.csv`
### Localização: `./backend/downloads/Integracao/`

### Estrutura do Arquivo:

```csv
tipo_despesa;razao_social;uf;total_despesas;media_despesas_trimestre;desvio_padrao_despesas;qtd_registros;qtd_trimestres;qtd_anos
Despesas com Sinistros;OPERADORA XYZ LTDA;SP;15000000.50;3750000.12;250000.00;120;4;3
Despesas com Sinistros;OPERADORA ABC SA;RJ;12500000.00;3125000.00;180000.00;100;4;3
Todas as Despesas;OPERADORA XYZ LTDA;SP;25000000.75;6250000.18;420000.00;200;4;3
...
```

### Campos do Arquivo:

1. **tipo_despesa**: "Despesas com Sinistros" ou "Todas as Despesas"
2. **razao_social**: Nome da operadora (obtido do campo DESCRICAO)
3. **uf**: Sigla da Unidade Federativa
4. **total_despesas**: Soma total das despesas (R$)
5. **media_despesas_trimestre**: Média de despesas por trimestre (R$)
6. **desvio_padrao_despesas**: Desvio padrão das despesas (R$)
7. **qtd_registros**: Quantidade de registros agrupados
8. **qtd_trimestres**: Quantidade de trimestres distintos
9. **qtd_anos**: Quantidade de anos distintos

## 🔄 Fluxo de Processamento

```
┌─────────────────────────────────────┐
│ Arquivo CSV de entrada              │
│ (consolidado_despesas_*.csv)        │
└──────────────┬──────────────────────┘
               │
               ▼
┌─────────────────────────────────────┐
│ Normalização de Colunas             │
│ - Uppercase                         │
│ - Identificação de UF               │
└──────────────┬──────────────────────┘
               │
               ▼
┌─────────────────────────────────────┐
│ Conversão de Valores                │
│ - VL_SALDO_INICIAL → numérico       │
│ - VL_SALDO_FINAL → numérico         │
│ - TOTAL_DESPESAS = VL_SALDO_FINAL   │
└──────────────┬──────────────────────┘
               │
               ▼
┌─────────────────────────────────────┐
│ Agrupamento                         │
│ GROUP BY: RAZAO_SOCIAL, UF          │
│                                     │
│ Cálculos:                           │
│ - SUM(TOTAL_DESPESAS)               │
│ - MEAN(TOTAL_DESPESAS)              │
│ - STD(TOTAL_DESPESAS)               │
│ - COUNT(*)                          │
│ - COUNT(DISTINCT TRIMESTRE)         │
│ - COUNT(DISTINCT ANO)               │
└──────────────┬──────────────────────┘
               │
               ▼
┌─────────────────────────────────────┐
│ Ordenação                           │
│ ORDER BY total_despesas DESC        │
└──────────────┬──────────────────────┘
               │
               ▼
┌─────────────────────────────────────┐
│ Salvamento em CSV                   │
│ despesas_agregadas.csv              │
└─────────────────────────────────────┘
```

## 📈 Exemplo de Saída no Console

```
==============================================================
GERAÇÃO DE DESPESAS AGREGADAS
==============================================================

1. Processando Despesas com Sinistros...
  ✓ Carregado: 5000 registros

  Agregando dados por Razão Social e UF...
  ✓ 150 grupos agregados
  ✓ Total geral de despesas: R$ 500,000,000.00

2. Processando Todas as Despesas...
  ✓ Carregado: 8000 registros

  Agregando dados por Razão Social e UF...
  ✓ 200 grupos agregados
  ✓ Total geral de despesas: R$ 850,000,000.00

  === RESUMO DOS RESULTADOS ===
  Total de registros agregados: 350
  Total geral de despesas: R$ 1,350,000,000.00
  Média geral: R$ 3,857,142.86
  Desvio padrão médio: R$ 245,000.00

  === TOP 5 MAIORES DESPESAS ===
  OPERADORA XYZ LTDA (SP) - R$ 25,000,000.00
  OPERADORA ABC SA (RJ) - R$ 22,500,000.00
  OPERADORA DEF LTDA (MG) - R$ 20,000,000.00
  OPERADORA GHI SA (RS) - R$ 18,750,000.00
  OPERADORA JKL LTDA (BA) - R$ 17,500,000.00

✓ Análise concluída com sucesso!
✓ 350 registros agregados salvos em: ./backend/downloads/Integracao/despesas_agregadas.csv
==============================================================
```

## 🚀 Como Executar

### Opção 1: Executar Sistema Completo
```bash
python Main.py
```

### Opção 2: Executar Apenas Transformação
```bash
python backend/2-transformacao_validacao/main.py
```

### Opção 3: Executar Apenas Agregação
```python
from backend.2-transformacao_validacao.casos_uso.gerar_despesas_agregadas import GerarDespesasAgregadas

gerador = GerarDespesasAgregadas("./backend/downloads/Integracao")
gerador.executar()
```

## 📊 Casos de Uso

### 1. Identificar Operadoras com Maiores Gastos
Analise o arquivo ordenado para identificar rapidamente as operadoras com maiores despesas por UF.

### 2. Análise de Variabilidade
Use o desvio padrão para identificar operadoras com despesas mais voláteis.

### 3. Comparação Regional
Compare despesas entre diferentes UFs para a mesma operadora.

### 4. Análise Temporal
Use qtd_trimestres e qtd_anos para entender a cobertura temporal dos dados.

### 5. Identificação de Padrões
Combine os dois tipos de despesas para análises comparativas:
- Proporção de sinistros no total de despesas
- Operadoras com sinistralidade alta

## ⚙️ Configurações

### Campos Opcionais de UF

O sistema busca automaticamente por colunas de UF com os seguintes nomes:
- `UF`
- `SG_UF`
- `SIGLA_UF`
- `ESTADO`

Se nenhuma for encontrada, usa 'N/A' como padrão.

### Tratamento de Valores Inválidos

- Valores não numéricos são convertidos para 0
- Registros com TOTAL_DESPESAS = 0 são removidos
- Desvio padrão NaN (1 registro) é convertido para 0

## 📋 Dependências

- **pandas**: Manipulação de dados e agregações
- **numpy**: Cálculos estatísticos

## ✅ Validações

O sistema valida:
- ✓ Existência dos arquivos de entrada
- ✓ Presença de colunas obrigatórias
- ✓ Conversão de valores numéricos
- ✓ Remoção de registros inválidos

## 🔍 Troubleshooting

### Erro: "Coluna obrigatória não encontrada"
**Solução**: Verifique se os arquivos CSV possuem as colunas: DESCRICAO, VL_SALDO_INICIAL, VL_SALDO_FINAL, TRIMESTRE, ANO

### Aviso: "Coluna UF não encontrada"
**Solução**: Normal se os dados não possuem UF. Sistema usa 'N/A' como padrão.

### Erro: "Nenhum registro válido após limpeza"
**Solução**: Verifique se os valores de VL_SALDO_FINAL não estão todos zerados ou inválidos.

## 📦 Arquivos do Módulo

```
backend/2-transformacao_validacao/
├── casos_uso/
│   └── gerar_despesas_agregadas.py  # Novo módulo
├── main.py                          # Atualizado com nova etapa
└── ...
```

## 🎯 Próximas Melhorias

- [ ] Adicionar gráficos de visualização
- [ ] Exportar também em formato Excel
- [ ] Adicionar filtros por período
- [ ] Gerar relatórios PDF automáticos
- [ ] Análise de tendências temporais
