# 2 - Transformação e Validação de Dados

Módulo responsável por ler, transformar, validar e importar os arquivos CSV gerados pela integração com API pública.

## Funcionalidades

- Leitura dos arquivos CSV consolidados
- Validação de dados (valores nulos, duplicados, campos essenciais)
- Análise estatística dos dados
- Relatórios de qualidade dos dados
- **Importação dos dados no banco de dados PostgreSQL**

## Arquivos de Entrada

Os arquivos são lidos de `./backend/downloads/Integracao/`:
- `consolidado_despesas_sinistros.csv`
- `consolidado_todas_despesas.csv`

## Estrutura do Módulo

### domain/
- **entidades.py**: Definições de DemonstracaoContabil e ResultadoImportacao
- **repositorios.py**: Interfaces abstratas para CSV e Banco

### infraestrutura/
- **repositorio_csv.py**: Implementação para leitura de arquivos CSV
- **repositorio_banco_dados.py**: Implementação para acesso ao banco PostgreSQL

### casos_uso/
- **importar_dados_consolidados.py**: Caso de uso para importação dos dados

## Banco de Dados

### Tabela: demonstracoes_contabeis_temp

A tabela é criada automaticamente durante a inicialização do banco (arquivo `/database/01-init.sql`).

**Campos:**
- `id`: Identificador único (SERIAL PRIMARY KEY)
- `data`: Data do registro
- `reg_ans`: Registro ANS (8 caracteres)
- `cd_conta_contabil`: Código da conta contábil (9 caracteres)
- `descricao`: Descrição da conta
- `vl_saldo_inicial`: Valor do saldo inicial
- `vl_saldo_final`: Valor do saldo final
- `trimestre`: Trimestre (1-4)
- `ano`: Ano
- `created_at`: Data de criação do registro

**Indices:**
- `idx_demonstracoes_data`: Índice em data
- `idx_demonstracoes_reg_ans`: Índice em reg_ans
- `idx_demonstracoes_conta`: Índice em cd_conta_contabil
- `idx_demonstracoes_trimestre_ano`: Índice em (trimestre, ano)

**Constraint:**
- `UNIQUE (reg_ans, cd_conta_contabil, trimestre, ano)`: Evita duplicatas

## Validações Realizadas

1. **Verificação de Valores Nulos**
   - Identifica colunas com valores ausentes
   - Quantifica registros incompletos

2. **Detecção de Duplicados**
   - Identifica registros duplicados
   - Quantifica ocorrências

3. **Validação de Campos Essenciais**
   - REG_ANS
   - CD_CONTA_CONTABIL
   - ANO
   - TRIMESTRE

4. **Análise Estatística**
   - Distribuição por ano e trimestre
   - Estatísticas de valores (min, max, média, mediana)

5. **Importação no Banco**
   - Inserção com tratamento de conflitos
   - Relatório de sucesso/erro por registro

## Como Executar

```bash
python main.py
```

Ou através do Main.py na raiz do projeto para executar ambas as funcionalidades.

## Saída Esperada

```
============================================================
TRANSFORMAÇÃO E VALIDAÇÃO DE DADOS
============================================================

1. Carregando dados...
✓ Encontrado: consolidado_despesas_sinistros.csv
✓ Encontrado: consolidado_todas_despesas.csv
  Despesas com Sinistros: XXX registros carregados
    Colunas: YY
  Todas as Despesas: ZZZ registros carregados
    Colunas: YY

2. Realizando validações...
  ...

3. Análise Estatística...
  ...

4. Importando dados no banco de dados...
============================================================
IMPORTAÇÃO DE DADOS CONSOLIDADOS
============================================================

1. Importando Despesas com Sinistros...
✓ consolidado_despesas_sinistros.csv: XXX registros carregados
✓ XXX registros inseridos com sucesso

2. Importando Todas as Despesas...
✓ consolidado_todas_despesas.csv: ZZZ registros carregados
✓ ZZZ registros inseridos com sucesso

============================================================
RESUMO DA IMPORTAÇÃO
============================================================
Total de registros processados: XXXX
Registros importados com sucesso: YYYY
Registros com erro: ZZ
Taxa de sucesso: 99.99%
Tempo de execução: 5.23s
============================================================

✓ Importação concluída com sucesso!

============================================================
TRANSFORMAÇÃO E VALIDAÇÃO CONCLUÍDA
============================================================
```

