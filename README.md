# Intuitive Care - Sistema de Consolidação de Demonstrações Contábeis da ANS

Uma solução completa de coleta, processamento, validação e consolidação de demonstrações contábeis de operadoras de planos de saúde a partir da API pública da ANS (Agência Nacional de Saúde Suplementar).

## Visão Geral

Este projeto automatiza o fluxo de integração com a API ANS, processando demonstrações contábeis de múltiplos trimestres, consolidando dados com informações de operadoras e gerando relatórios enriquecidos. O sistema é executado completamente dentro de containers Docker, garantindo consistência e isolamento de dependências entre diferentes ambientes.

---

## Desafios Enfrentados e Decisões Técnicas

### Contexto do Desenvolvimento

O sistema foi desenvolvido para resolver um desafio real: consolidar demonstrações contábeis de múltiplas fontes (API ANS) com dados operacionais, mantendo integridade e rastreabilidade completa. Durante este percurso, várias decisões técnicas críticas foram necessárias.

### Desafio 1: Dados Incompletos na Fonte Primária

**Problema Encontrado:**
As demonstrações contábeis baixadas da API ANS **não continham CNPJ e Razão Social**. O arquivo continha apenas:
- Trimestre e Ano
- Registro da Operadora (código numérico)
- Código e Descritivo do Plano de Contas
- Valores de Despesas

**Impacto:**
Impossível gerar relatórios consolidados sem informações de identificação das operadoras.

**Solução Implementada:**
1. Download adicional de tabela de Operadoras da API ANS
2. Carregamento de operadoras ativas (1.110) e inativas (3.046)
3. Consolidação via LEFT JOIN na coluna `reg_ans`

**Justificativa:**
- Inclusão de operadoras inativas: operadora pode ter ficado inativa entre trimestres, causando perda de contexto histórico
- LEFT JOIN preferido a INNER JOIN: preserva registros órfãos para auditoria

---

### Desafio 2: Volume de Dados - Trade-off Processamento em Memória vs Incremental

**Problema Encontrado:**
Sistema precisava processar 28.989 registros de demonstrações contábeis + 4.156 operadoras + múltiplos trimestres.

**Opções Consideradas:**

#### Opção A: Processamento Totalmente em Memória
```
Vantagens:
- Velocidade de processamento (tudo em RAM)
- Operações JOIN simples em Pandas
- Menos I/O disco

Desvantagens:
- Consumo de memória elevado (dados podem crescer)
- Risco de crash em caso de memória insuficiente
- Impossível retomar em caso de falha
- Difícil escalar para volumes maiores
```

#### Opção B: Processamento Incremental em Banco de Dados (ESCOLHIDA)
```
Vantagens:
- Consumo de memória constante
- Recuperação automática em caso de falha (checkpoint)
- Escalável para volumes muito maiores
- JOINs otimizados pelo PostgreSQL
- Auditoria completa via banco de dados
- Dados persistidos imediatamente

Desvantagens:
- Mais operações I/O
- Complexidade adicional (gerenciar banco)
- Overhead de conexão com DB
```

**Decisão Justificada:**
Optou-se pela **Opção B (Incremental em Banco)** pelos seguintes motivos:

1. **Volume de Dados:** 28.989 registros é significativo. Com crescimento, ultrapassaria capacidade de memória
2. **Recuperação em Falha:** Implementar checkpoint permite retomada sem reprocessamento
3. **Auditoria:** Banco de dados fornece rastreabilidade melhor que arquivos em memória
4. **Reusabilidade:** Dados no banco podem ser consultados depois para análises adicionais
5. **Performance de JOIN:** PostgreSQL otimiza melhor que Pandas para grandes volumes
6. **Manutenibilidade:** Separação clara entre camadas de dados e processamento

**Implementação:**
- Dados inseridos em `demonstracoes_contabeis_temp` conforme processados
- Operadoras carregadas em tabela `operadoras`
- JOINs executados via SQL (não Pandas)
- Resultado persistido em CSV para saída final

**Métricas de Execução:**
- Tempo de processamento: ~5-15 minutos (dependente de internet)
- Consumo de memória: < 500MB durante execução
- Tamanho do banco: ~50MB com dados completos

---

### Desafio 3: Inconsistências Encontradas Durante Consolidação

Durante o processamento, várias inconsistências de dados foram encontradas. Cada uma recebeu tratamento específico baseado em análise crítica:

#### Inconsistência A: CNPJ Duplicado com Razões Sociais Diferentes

**Cenário Encontrado:**
```
Mesmo CNPJ = 01234567000190
  Registro 1: Razão Social = "SAUDE PLENA S/A"
  Registro 2: Razão Social = "SAUDE PLENA LTDA"
  Registro 3: Razão Social = "SAUDE PLENA"
```

**Causa Raiz:**
- Alterações na razão social ao longo do tempo
- Erros na digitação em diferentes trimestres
- Fusões/incorporações com registro não atualizado

**Abordagem Implementada: MANTER TODAS AS VARIAÇÕES**

```
Ação: Registros não são deduplados
Justificativa:
1. Impossível determinar qual é "correta" sem análise manual
2. Pode representar situações legítimas (mudança de nome social)
3. Auditoria requer preservação de histórico
4. Gera flag "suspeito" em log para revisão manual

Resultado:
- Consolidado inclui todas as variações
- Log marca como "CNPJ com múltiplas razões sociais"
- Usuário pode investigar manualmente se necessário
```

**Tratamento Técnico:**
```python
# No repositório_banco_dados.py:
# Ao inserir operadoras, INSERT ... ON CONFLICT DO NOTHING
# (permite múltiplas razões sociais para mesmo CNPJ)

# No log de auditoria:
# Registra: "CNPJ [12345678000190] contém 3 variações de razão social"
```

**Impacto nos Relatórios:**
- CSV consolidado mostra todas as combinações
- Usuário vê todas as variações
- Facilita identificação de problemas de dados

---

#### Inconsistência B: Valores Zerados ou Negativos

**Cenário Encontrado:**
```
Trimestre: 2T2025
Registro Operadora: 12345
Descritivo: DESPESAS COM EVENTOS/SINISTROS
Valor: 0.00  ← PROBLEMÁTICO
Valor: -150.50  ← DEDUCAO? OU ERRO?
```

**Causa Raiz:**
- Valores zero podem ser "não reportado" vs "realmente zero"
- Valores negativos podem ser deduções legítimas OU erros de digitação
- Trimestres sem despesa ainda geram registros

**Abordagem Implementada: ESTRATÉGIA EM DUAS CAMADAS**

```
Camada 1 - VALIDAÇÃO NA INSERÇÃO:
  Se valor = 0:
    → Ignorado (não inserido)
    Justificativa: Zero não representa despesa, apenas ruído
    
  Se valor < 0:
    → INSERIDO, SEM marcação especial
    Justificativa: Valores negativos representam DEDUCOES LEGÍTIMAS
    Exemplos encontrados nos dados reais:
      • "(-) Provisão para Perdas Sobre Créditos" = -71.495,56
      • "(-) Recuperação por Co-Participação" = -42,00
      • "(-) Glosas" = -2,00
      • "(-) Deduções sobre Despesas com Sinistros" = variável
    
Camada 2 - PRESERVAÇÃO NA SAÍDA:
  Arquivo "consolidado_todas_despesas.csv":
    → Inclui valores negativos (8.251 registros com valor < 0)
    → Representam deduções legitimamente registradas nos dados da API
    → Identificáveis pelo prefixo "(-)" no descritivo da conta
    
  Arquivo "consolidado_despesas_sinistros.csv":
    → Inclui valores negativos quando aplicável
    → 337 registros relacionados a "DESPESAS COM EVENTOS/SINISTROS"
```

**Implementação Técnica:**
```python
# Validação em carregar_dados_banco.py:
if valor == 0:
    logger.warning(f"Registro ignorado: valor zero em {reg_ans}")
    continue  # Não insere

# Valores negativos são SEMPRE inseridos sem filtro
# pois representam deduções legítimas nas contas contábeis
if valor < 0:
    logger.debug(f"Deducao encontrada: {reg_ans} ({descritivo}) = {valor}")
    # Insere normalmente - não há restrição para negativos
```

**Impacto nos Relatórios:**
- Valores zerados não aparecem (reduz ruído)
- Valores negativos aparecem explicitamente (auditabilidade)
- Usuário pode analisar deduções separadamente nos CSVs

---

#### Inconsistência C: Trimestres com Formatos Inconsistentes

**Cenário Encontrado:**
```
API retorna trimestres em formatos variados:
  "2025-01" (ISO)
  "1T2025"  (Brasileiro)
  "Q1/2025" (English)
  "01/2025" (MM/YYYY)
  "2025 T1" (Com espaço)
```

**Causa Raiz:**
- API não especifica formato padrão
- Possíveis mudanças de versão de API
- Dados de diferentes períodos históricos

**Abordagem Implementada: NORMALIZAÇÃO AUTOMÁTICA**

```
Lógica de Conversão:
1. Detectar formato automaticamente
2. Converter para padrão interno: "1T" + ano
3. Validar se trimestre está em intervalo [1T-4T]
4. Registrar em log conversões realizadas

Exemplo:
  Entrada: "2025-01"
  Detecção: Formato ISO
  Conversão: "1T2025"
  Validação: OK (1T válido)
  Uso interno: "1T"
```

**Implementação Técnica:**
```python
# Em carregar_operadoras.py:
def normalizar_trimestre(trimestre_raw):
    """Converte qualquer formato para '1T', '2T', '3T', '4T'"""
    
    # Remover espaços
    trimestre = trimestre_raw.strip()
    
    # Formato "1T2025" - já normalizado
    if re.match(r'[1-4]T\d{4}', trimestre):
        return trimestre[:2]  # Retorna "1T"
    
    # Formato "2025-01"
    if re.match(r'\d{4}-\d{2}', trimestre):
        mes = int(trimestre.split('-')[1])
        trimestre_num = (mes - 1) // 3 + 1
        return f"{trimestre_num}T"
    
    # ... outros formatos
    
    logger.warning(f"Formato desconhecido: {trimestre_raw}")
    return None
```

**Validação Adicional:**
```
Após normalização:
1. Verificar se trimestre está em [1T, 2T, 3T, 4T]
2. Verificar se ano é válido (>= 2015, <= 2026)
3. Registrar em log todas as conversões
4. Marcar como "suspeito" se conversão incerta
```

**Impacto nos Relatórios:**
- Todos os trimestres em formato consistente
- Relatórios agrupáveis por trimestre sem conversão
- Log documenta quais dados foram convertidos
- Facilita comparações entre períodos

---

### Síntese das Decisões Críticas

| Decisão | Opção Escolhida | Justificativa Principal |
|---------|-----------------|------------------------|
| **Processamento** | Incremental em DB | Escalabilidade, recuperação e auditoria |
| **CNPJ Duplicado** | Manter variações | Impossível determinar corretude, preserva auditoria |
| **Valores Zerados** | Ignorar (validação pré-insert) | Zero não representa despesa real |
| **Valores Negativos** | Preservar sempre | São deduções legítimas nas contas (8.251 encontradas) |
| **Trimestres** | Normalizar automaticamente | Múltiplos formatos na API, conversão lógica |
| **JOIN** | LEFT (não INNER) | Preserva registros órfãos para auditoria |
| **Logs** | Por sessão (não global) | Rastreabilidade individual de cada execução |

---

## Estrutura do Projeto

```
Teste_Jessica_jabes/
├── docker-compose.yml                        # Orquestração de containers
├── README.md                                  # Esta documentação
├── executar_interativo.ps1                   # Script PowerShell para execução
├── Main.py                                    # Ponto de entrada principal
├── database/
│   └── 01-init.sql                           # Schema PostgreSQL com tabelas
├── backend/
│   └── 1-integracao_api_publica/
│       ├── main.py                           # Orquestração principal
│       ├── config.py                         # Configurações e variáveis
│       ├── requirements.txt                  # Dependências Python
│       ├── Dockerfile                        # Imagem do container
│       ├── casos_uso/
│       │   ├── buscar_trimestres.py          # Download de trimestres da API
│       │   ├── baixar_arquivos.py            # Extração de arquivos ZIP
│       │   ├── carregar_operadoras.py        # Processamento de operadoras
│       │   └── carregar_dados_banco.py       # Consolidação e JOIN
│       ├── domain/
│       │   ├── entidades.py                  # Modelos de dados
│       │   └── repositorios.py               # Interfaces de repositório
│       ├── infraestrutura/
│       │   ├── repositorio_banco_dados.py    # Implementação DB (inserts, JOINs)
│       │   ├── logger.py                     # Configuração centralizada de logs
│       │   └── gerenciador_checkpoint.py     # Persistência de progresso
│       ├── logs/                             # Arquivos de log por sessão
│       ├── downloads/                        # Arquivos baixados da API
│       │   └── Integracao/
│       │       ├── operadoras_*.csv          # Operadoras ativas/canceladas
│       │       ├── *.xlsx, *.csv, *.txt      # Demonstrações contábeis
│       │       └── checkpoints/
│       │           └── progresso.json        # Estado de recuperação
│       └── consolidados/                     # Saída final (CSVs e ZIP)
└── .env                                      # Variáveis de ambiente
```

## Funcionalidades Implementadas

### Modulo 1: Integração com API Pública ANS

#### 1.1 Busca e Download de Trimestres
- Consulta automática dos últimos 3 trimestres disponíveis (2025/1T, 2025/2T, 2025/3T)
- Download direto da API da ANS
- Validação de integridade do arquivo após download
- Registro detalhado em logs

#### 1.2 Extração de Demonstrações Contábeis
- Descompactação automática de arquivos ZIP
- Suporte a múltiplos formatos: CSV, XLSX, TXT
- Filtro automático para arquivos contendo "DESPESAS COM EVENTOS/SINISTROS"
- Validação de existência e integridade dos arquivos antes do processamento

#### 1.3 Processamento de Operadoras
- Download de 2 tabelas da API: operadoras ativas e canceladas
- Total de 4.156 operadoras carregadas
- Normalização de campos de registro
- Identificação de situação (ativa/inativa) para matching temporal

#### 1.4 Consolidação de Dados via JOIN
- Importação de demonstrações (28.989 registros) em tabela PostgreSQL
- Importação de operadoras (4.156 registros) em tabela PostgreSQL
- Execução de LEFT JOIN otimizado na coluna 'reg_ans'
- Resultado: 83.183 registros consolidados com CNPJ e Razão Social

#### 1.5 Sistema de Checkpoint e Recuperação
- Persistência de progresso após cada trimestre processado
- Formato: JSON com status e contador de registros
- Permite retomada exata em caso de falha
- Sem reprocessamento de dados já concluídos

#### 1.6 Logging Detalhado por Sessão
- Criação de arquivo de log individual: `sessao_YYYYMMDD_HHMMSS.log`
- Registro de todas operações, erros e inconsistências
- Fallback automático para diretório `/tmp` se local padrão indisponível
- Inclusão automática do log na saída ZIP

### Modulo 2: Transformação e Validação
- Leitura dos CSVs consolidados
- Análise estatística de dados
- Detecção de inconsistências
- Geração de relatórios de qualidade

---

## Fluxo Detalhado de Processamento

### Etapa 1: Busca de Trimestres
O sistema consulta a API da ANS e identifica os últimos 3 trimestres com demonstrações contábeis disponíveis. Para cada trimestre, um arquivo ZIP é baixado podendo conter múltiplas demonstrações em diferentes formatos.

**Validações:**
- URL acessível e retorna código 200
- Arquivo não vazio após download
- Arquivo é válido para descompactação

### Etapa 2: Extração de Arquivos
Os ZIPs são descompactados e seus arquivos são analisados. O sistema identifica quais arquivos contêm dados de "DESPESAS COM EVENTOS/SINISTROS" conforme requisitado.

**Formatos Suportados:** CSV, XLSX, TXT

**Validação Pré-Processamento:**
- Arquivo existe e não está vazio
- Headers/estrutura esperada presente
- Arquivo não está corrompido

### Etapa 3: Normalização de Dados

Os dados de múltiplas fontes são normalizados para formato padrão:

**Normalização de Nomes de Coluna:**
- Conversão para snake_case
- Remoção de espaços em branco
- Tratamento de caracteres especiais
- Exemplo: "REGISTRO_OPERADORA" → "reg_ans"

**Normalização de Valores Numéricos:**
- Separador decimal: virgula (,) convertida para ponto (.)
- Milhares: remoção de separadores
- Exemplo: "1.234,50" → 1234.50

**Normalização de Datas:**
- Aceita formatos: YYYY-MM-DD (ISO, preferido), DD/MM/YYYY, DD-MM-YYYY
- Converte para formato ISO padrão
- Registra em log datas que não conversão

**Normalização de Registro de Operadora:**
- Campo obrigatório deve ser numérico e não vazio
- Tamanho: 8 caracteres (VARCHAR(8))
- Remove valores inválidos: NaN, vazio, não numérico

**Normalização de Regime:**
- Padronização de valores: Adesão, Coletivo Empresarial, Coletivo por Adesão, Individual/Familiar

### Etapa 4: Validações e Tratamento de Inconsistências

O sistema implementa estratégias diferenciadas para cada tipo de problema encontrado:

#### Estratégia: IGNORAR
- **Registros com valor = 0**: Não representam despesa real
- **Datas inválidas**: Impossíveis de converter para formato padrão
- **Registro operadora inválido**: NaN, vazio, não numérico
- **Campos obrigatórios vazios**: Sem dados essenciais

Ação: Registro descartado, operação registrada em log

#### Estratégia: CORRIGIR
- **Espaços em branco**: Remoção de início/fim
- **Separador decimal**: Virgula convertida para ponto
- **Formato de data**: Conversão de múltiplos formatos para ISO
- **Case em texto**: Padronização (maiúscula/minúscula)

Ação: Valor corrigido e utilizado, operação registrada em log

#### Estratégia: MARCAR COMO SUSPEITO
- **Multiplicidade no JOIN**: Mais de uma operadora com mesmo registro
- **Registros órfãos**: Registro operadora não encontra match
- **Valores extremos**: Muito acima/abaixo do intervalo esperado

Ação: Dado incluído na saída com flag de suspeita, registrado em log para revisão manual

### Etapa 5: Inserção em Banco de Dados

Os dados validados e normalizados são inseridos em tabelas PostgreSQL:

**Tabela `demonstracoes_contabeis_temp`:**
- Armazena: trimestre, ano, reg_ans, cd_conta_contabil, valor
- Estratégia de insert: INSERT ... ON CONFLICT DO UPDATE
- Mantém histórico sem duplicação

**Tabela `operadoras`:**
- Armazena: reg_ans (PK), razao_social, cnpj, status (ativa/cancelada)
- Estratégia de insert: INSERT ... ON CONFLICT DO UPDATE
- Validação: Ignora registros com NaN em reg_ans

### Etapa 6: Consolidação via LEFT JOIN

Como as demonstrações contábeis originais não continham CNPJ e Razão Social:

1. **Download de Operadoras**: Tabelas ativas (1.110) e inativas (3.046) somam 4.156 registros
2. **Processamento Temporal**: Inclui inativas pois operadora pode ter ficado inativa entre trimestres
3. **JOIN de Consolidação**: LEFT JOIN em demonstracoes LEFT JOIN operadoras ON reg_ans

Resultado: 83.183 registros com informações completas

### Etapa 7: Tratamento de Casos Especiais no JOIN

#### Registros com Operadora Encontrada (Match Único)
- Inclusos em ambos CSVs (todas_despesas e sinistros)
- Contém: CNPJ, razao_social, trimestre, ano, regime, subdemento, valor
- Confiabilidade: ALTA

#### Registros Órfãos (Sem Match)
- Inclusos em ambos CSVs
- Colunas CNPJ e razao_social ficam vazias
- Causas possíveis: registro invalido, operadora nunca existiu, erro de dados
- Registrados em log para identificação

#### Registros com Multiplicidade (Mais de uma Operadora)
- Inclusos em ambos CSVs (sem duplicar linhas)
- Campos CNPJ e razao_social são marcados como "DUPLICIDADE"
- Causas possíveis: erro histórico no banco, operadora duplicada

### Etapa 8: Geração de CSVs de Saída

**CSV 1: consolidado_todas_despesas.csv**
- Registros: 83.183 (todos com operadora encontrada)
- Colunas: reg_ans, razao_social, cnpj, trimestre, ano, regime, subdescritivo, valor
- Uso: Análise completa de despesas
- Filtro: Nenhum (dados de todos os arquivos processados)

**CSV 2: consolidado_despesas_sinistros.csv**
- Registros: 337 (apenas com "DESPESAS COM EVENTOS/SINISTROS")
- Colunas: mesmas acima + deducao (se houver)
- Uso: Análise focada em sinistros
- Filtro: Apenas registros onde descritivo contém "DESPESAS COM EVENTOS/SINISTROS"

Ambos os CSVs são inclusos em ZIP junto com o log da sessão.

### Etapa 9: Logging e Rastreabilidade

Cada execução gera arquivo de log único com timestamp:

**Arquivo:** `logs/sessao_YYYYMMDD_HHMMSS.log`

**Informações Registradas:**
- Início e fim de cada etapa com timestamp
- Quantidade de registros: baixados, processados, validados, inseridos
- Erros e avisos: ignorados, corrigidos, suspeitos
- Tempo de execução por operação
- Detalhes de JOIN: matches únicos, orfãos, multiplicidades
- Fallback de logging se diretório padrão indisponível

**Garantia de Rastreabilidade:** Nenhum dado fica perdido. Tudo está registrado em arquivo de import ou em log.

### Etapa 10: Packaging de Saída

Todos os arquivos são compactados em ZIP para fácil distribuição:

**Arquivo:** `consolidados/consolidado_despesas.zip`

**Conteúdo:**
- consolidado_todas_despesas.csv (83.183 registros)
- consolidado_despesas_sinistros.csv (337 registros)
- sessao_YYYYMMDD_HHMMSS.log (logs da sessão)

### Etapa 11: Sistema de Checkpoint

O progresso é persistido após cada trimestre:

**Arquivo:** `checkpoints/progresso.json`

**Formato:**
```json
{
  "1T": {"status": "concluido", "registros_processados": 8200},
  "2T": {"status": "processando", "registros_processados": 5100},
  "3T": {"status": "nao_iniciado"}
}
```

**Benefício:** Se execução for interrompida, próxima execução retoma exatamente de onde parou sem reprocessar dados.

---

## Validações Detalhadas

### Validação de Valor
**Regra:** Deve ser diferente de zero
- Aplica a: campo de valor das despesas
- Ação: Registros com valor = 0 são ignorados
- Justificativa: Zero não representa despesa, não adiciona valor analítico

### Validação de Data
**Formatos aceitos:** YYYY-MM-DD, DD/MM/YYYY, DD-MM-YYYY
- Conversão automática entre formatos
- Ação em falha: Registro ignorado
- Registro: Data inválida marcada como suspeita em log

### Validação de Registro Operadora
**Regra:** Deve ser numérico, não vazio, 8 caracteres
- Valores inválidos: NaN, vazio, não numérico
- Ação: Registros com registro inválido são ignorados
- Nota: Validado também durante INSERT para evitar dados corrompidos

### Validação de Descritivo do Plano de Contas
**Regra:** Obrigatório para filtro de "DESPESAS COM EVENTOS/SINISTROS"
- Registros sem descritivo: inclusos apenas em "todas_despesas"
- Registros com descritivo contendo "DESPESAS COM EVENTOS/SINISTROS": vão para "sinistros"

### Validação de Regime
**Regra:** Deve ser um dos valores esperados
- Valores válidos: Adesão, Coletivo Empresarial, Coletivo por Adesão, Individual/Familiar
- Ação em falha: Registro ignorado ou marcado como suspeito

---

## Benefícios da Arquitetura

- **Isolamento de Ambiente**: Containers garantem execução uniforme em qualquer máquina
- **Recuperação Automática**: Checkpoint permite retomar sem perda de dados
- **Rastreabilidade Completa**: Logs por sessão registram todas operações
- **Otimização de Performance**: JOINs no banco de dados (não em memória)
- **Validação em Múltiplas Camadas**: Antes, durante e após processamento
- **Modularidade**: Cada etapa é independente e testável

---

## Arquivo de Configuração (.env)

### Aviso de Segurança

O arquivo `.env` foi enviado apenas para facilitar testes locais.

NUNCA commit o arquivo `.env` ao repositório Git em produção. Este arquivo contém:
- Credenciais de banco de dados
- Chaves de API
- Tokens de autenticação
- Outras informações sensíveis

### Como Usar

1. O arquivo `.env` pré-preenchido foi fornecido para teste
2. Para ambiente de produção, crie um novo `.env` com suas próprias credenciais
3. Adicione `.env` ao `.gitignore` para evitar vazamento de dados

---

## Instalação e Execução

### Pré-requisitos

- Docker (versão 20.10 ou superior)
- Docker Compose (versão 1.29 ou superior)
- Sistema Operacional: Windows, Linux ou macOS

### Passo 1: Preparar Ambiente

```powershell
# Navegue até o diretório do projeto
cd C:\Users\jessi\Documents\PROJETOS\Teste_Jessica_Jabes
```

### Passo 2: Iniciar os Containers

```powershell
# Inicia PostgreSQL e container da aplicação
docker-compose up -d

# Verifica se os containers estão rodando
docker-compose ps
```

Você deverá ver:
- Container intuituve-care-postgres (PostgreSQL)
- Container intuitive-care-integracao-api (Aplicação)

### Passo 3: Executar o Script Principal

```powershell
# Executa a integração com API ANS
docker exec -i intuitive-care-integracao-api python /app/1-integracao_api_publica/main.py
```

**Tempo esperado de execução:** 5-15 minutos (dependendo de velocidade de internet e recursos)

### Passo 4: Verificar Resultados

```powershell
# Listar arquivos gerados
Get-ChildItem -Path "backend\1-integracao_api_publica\consolidados\"

# Listar logs gerados
Get-ChildItem -Path "backend\1-integracao_api_publica\logs\"
```

Você deve encontrar:
- `consolidado_despesas.zip` (arquivo principal com CSVs e log)
- `sessao_YYYYMMDD_HHMMSS.log` (log detalhado da execução)

### Passo 5: Parar os Containers

```powershell
# Para containers mantendo dados
docker-compose stop

# Remove containers (dados persistem no banco)
docker-compose down

# Remove containers E dados do banco (reset completo)
docker-compose down -v
```

### Opção Alternativa: Execução PowerShell Interativa

Um script PowerShell foi criado para facilitar a execução:

```powershell
# No Windows PowerShell (como Administrador)
.\executar_interativo.ps1
```

Este script:
- Verifica instalação do Docker
- Inicia containers
- Executa o processamento
- Mostra progresso em tempo real
- Oferece opções de parar/reiniciar

## Arquivos Gerados

Todos os arquivos de saída estão em `backend/1-integracao_api_publica/consolidados/`:

### Arquivo Principal
- **consolidado_despesas.zip** - Compactação contendo:
  - consolidado_todas_despesas.csv (83.183 registros)
  - consolidado_despesas_sinistros.csv (337 registros)
  - sessao_YYYYMMDD_HHMMSS.log (logs da sessão)

### Arquivos de Log
- **logs/sessao_YYYYMMDD_HHMMSS.log** - Log individual da execução (também incluído no ZIP)
- Registra: operações, erros, inconsistências, tempo de execução

### Arquivo de Checkpoint
- **checkpoints/progresso.json** - Estado de recuperação em JSON
- Permite retomada em caso de falha

### Estrutura de Dados Processados

**consolidado_todas_despesas.csv:**
```
reg_ans, razao_social, cnpj, trimestre, ano, regime, subdescritivo, valor
...
```

**consolidado_despesas_sinistros.csv:**
```
reg_ans, razao_social, cnpj, trimestre, ano, regime, subdescritivo, valor, deducao
...
```

---

## Resumo de Dados Processados

- **Trimestres processados**: 3 (2025/1T, 2025/2T, 2025/3T)
- **Registros baixados**: 28.989 demonstrações contábeis
- **Operadoras carregadas**: 4.156 (1.110 ativas + 3.046 canceladas)
- **Registros consolidados**: 83.183 (com operadora encontrada)
- **Registros de sinistros**: 337 (com "DESPESAS COM EVENTOS/SINISTROS")
- **Registros com valores negativos**: 8.251 (deduções legítimas)
- **Taxa de sucesso no JOIN**: 100% de matches únicos encontrados

---

## Casos Especiais Tratados

### Registros Órfãos (Sem Operadora)
Quando um registro de despesa possui `reg_ans` que não encontra match:
- Incluído no CSV "todas_despesas"
- Colunas CNPJ e razao_social ficam vazias
- Registrado em log para investigação
- Causas: registro inválido, operadora nunca existiu, erro de dados

### Registros com Multiplicidade
Quando um `reg_ans` encontra mais de uma operadora:
- NÃO incluído em nenhum CSV (evita duplicidade)
- Detalhes registrados em log
- Causas: erro no banco de operadoras, operadora duplicada

### Valores com Diferentes Formatos Decimais
- Entrada: "1.234,50" (brasileiro)
- Saída: 1234.50 (padrão)
- Processamento: Automático sem perda de precisão

### Operadoras Inativas no Período
- Motivo: Operadora pode ter ficado inativa entre os trimestres
- Solução: Sistema inclui tanto ativas quanto inativas no JOIN
- Resultado: Dados históricos preservados corretamente

---

## Variáveis de Ambiente

O arquivo `.env` contém:

```env
# Database
DB_HOST=intuitive-care-postgres
DB_PORT=5432
DB_NAME=intuitive_care
DB_USER=jessica
DB_PASSWORD=1234

# API ANS
ANS_API_URL=https://www.ans.gov.br/...

# Paths
LOG_DIR=/app/downloads/Integracao/logs
CHECKPOINT_DIR=/app/downloads/Integracao/checkpoints
CONSOLIDADOS_DIR=/app/1-integracao_api_publica/consolidados
```

### Aviso de Segurança

Em produção, NUNCA faça commit do arquivo `.env`:
1. Crie `.env` localmente apenas
2. Adicione `.env` ao `.gitignore`
3. Use variáveis de ambiente do sistema ou secrets manager

---

## Comandos Úteis

### Visualizar Logs em Tempo Real
```powershell
docker-compose logs -f intuitive-care-integracao-api
```

### Acessar Container (Bash)
```powershell
docker exec -it intuitive-care-integracao-api bash
```

### Acessar Banco PostgreSQL
```powershell
docker exec -it intuitive-care-postgres psql -U jessica -d intuitive_care

# Dentro do psql:
SELECT COUNT(*) FROM demonstracoes_contabeis_temp;
SELECT COUNT(*) FROM operadoras;
```

### Reconstruir Imagens (se código foi alterado)
```powershell
docker-compose build
docker-compose up -d
```

### Reset Completo (CUIDADO - Remove dados!)
```powershell
docker-compose down -v
docker-compose up -d
docker exec -i intuitive-care-integracao-api python /app/1-integracao_api_publica/main.py
```

### Extrair CSV do ZIP
```powershell
# Abrir o ZIP diretamente ou:
Expand-Archive -Path "backend\1-integracao_api_publica\consolidados\consolidado_despesas.zip" `
               -DestinationPath "backend\1-integracao_api_publica\consolidados\extraido\"
```

---

## Estratégia de Logging

### Logs por Sessão

O sistema gera um arquivo de log para cada execução com timestamp único:
- **Formato:** `sessao_YYYYMMDD_HHMMSS.log`
- **Localização:** `backend/1-integracao_api_publica/logs/`
- **Encoding:** UTF-8 com suporte a caracteres especiais

**Exemplo:**
```
sessao_20260131_143052.log
sessao_20260131_150124.log
```

### Níveis de Log Utilizados

| Nível | Uso | Exemplo |
|-------|-----|---------|
| **INFO** | Progresso normal do processamento | "Arquivo extraído com sucesso" |
| **WARNING** | Situações que não impedem execução | "Erro na normalização numérica", "Operadora não localizada" |
| **ERROR** | Erros que impedem parte do processamento | "Falha ao ler arquivo", "Erro ao inserir no banco" |
| **DEBUG** | Informações técnicas detalhadas (desabilitado em produção) | Detalhes internos de conversão |

### O Que É Registrado

✅ **Sempre Registrado:**
- Início e fim do processamento
- Total de arquivos processados
- Total de registros inseridos/com erro
- Erros na conversão de dados (normalização numérica, datas inválidas)
- Registros órfãos (sem operadora correspondente)
- Duplicidades detectadas no JOIN
- Valores do comparativo (inicial vs final)
- Falhas em leitura de arquivos
- Problemas de conexão com banco de dados

❌ **NÃO Registrado (Por Performance):**
- **Normalização numérica bem-sucedida:** São milhares de conversões (todas as linhas, todos os campos numéricos)
  - Motivo: Gera volume excessivo de log sem valor diagnóstico
  - Apenas erros de normalização são registrados
- **Validações de campos bem-sucedidas:** Maioria dos registros passa nas validações
- **Operações de banco bem-sucedidas:** Apenas falhas são registradas

### Exemplo de Log

```
2026-01-31 14:30:52,123 | INFO     | main.py:65 | principal() | Iniciando Integração de Dados da API Pública ANS
2026-01-31 14:31:15,456 | INFO     | carregar_dados_banco.py:145 | _extrair_dados_arquivo() | Dados extraídos com sucesso: 1T2025.csv - 9876 registros válidos de 9876 linhas
2026-01-31 14:31:45,789 | WARNING  | repositorio_banco_dados.py:235 | _normalizar_numero() | Erro na normalização numérica (campo=VL_SALDO_FINAL, arquivo=1T2025.csv, linha=523, reg_ans=12345): valor='INVALIDO' - could not convert string to float
2026-01-31 14:32:10,234 | ERROR    | carregar_dados_banco.py:155 | _extrair_dados_arquivo() | Falha ao ler arquivo - nenhum encoding funcionou: arquivo_corrompido.csv
2026-01-31 14:35:42,567 | INFO     | carregar_dados_banco.py:175 | executar() | Comparativo: Inicial=5005672899427.1, Final=5005672899427.1, Diferença=0.0, Percentual=0.00%
```

### CSV de Erros

Além dos logs, um CSV detalhado de erros é gerado:
- **Arquivo:** `backend/1-integracao_api_publica/downloads/Integracao/erros/erros_insercao.csv`
- **Conteúdo:** Todos os registros que falharam nas validações ou inserção
- **Colunas:** arquivo_origem, linha_arquivo, timestamp, reg_ans, cd_conta_contabil, motivo_erro, tipo_erro

**Resumo em Texto:**
- **Arquivo:** `erros_resumo.txt`
- **Conteúdo:** Estatísticas agregadas de erros por tipo e motivo

---

## Troubleshooting

### Erro: "Connection refused" no PostgreSQL
**Causa:** Container do banco não está ativo
**Solução:**
```powershell
docker-compose restart
```

### Erro: "No such file or directory" em logs
**Causa:** Diretório de logs não foi criado
**Solução:** Será criado automaticamente, ou manualmente:
```powershell
mkdir "backend\1-integracao_api_publica\logs"
```

### Erro: "Value too long for VARCHAR(8)"
**Causa:** Registro operadora com mais de 8 caracteres
**Solução:** Implementado - sistema agora valida e converte para VARCHAR(8)

### Dados não aparecem em consolidados
**Verificar:**
1. Arquivo de log: `logs/sessao_*.log`
2. Console output do container
3. Estado do checkpoint: `checkpoints/progresso.json`

```powershell
# Ver últimas 50 linhas do log
docker exec intuitive-care-integracao-api tail -50 /app/downloads/Integracao/logs/sessao_*.log
```

### ZIP gerado vazio
**Causa:** Processamento completado mas sem dados
**Verificar:**
- Log da sessão para erros
- Se operadoras foram carregadas
- Se registros foram inseridos em banco

---

## Informações de Acesso ao Banco

Para análise direta dos dados:

**Acesso**
- Host: localhost
- Porta: 55432
- Usuário: jessica
- Senha: 1234
- Banco: intuitive_care

**Via Container (Recomendado):**
```powershell
docker exec -it intuitive-care-postgres psql -U jessica -d intuitive_care
```

**Queries Úteis:**
```sql
-- Total de registros por trimestre
SELECT trimestre, COUNT(*) FROM demonstracoes_contabeis_temp GROUP BY trimestre;

-- Operadoras com mais despesas
SELECT reg_ans, razao_social, SUM(valor) as total_despesas 
FROM (SELECT * FROM demonstracoes_contabeis_temp 
      LEFT JOIN operadoras USING(reg_ans)) 
GROUP BY reg_ans, razao_social 
ORDER BY total_despesas DESC LIMIT 10;

-- Despesas de sinistros por trimestre
SELECT trimestre, COUNT(*), SUM(valor) 
FROM demonstracoes_contabeis_temp 
WHERE descritivo LIKE '%DESPESAS COM EVENTOS%' 
GROUP BY trimestre;
```

---

## Stack Técnico

- **Linguagem:** Python 3.9+
- **Framework ORM:** SQLAlchemy 2.0+
- **Processamento de Dados:** Pandas 2.1+
- **Banco de Dados:** PostgreSQL 15
- **Driver DB:** psycopg2-binary 2.9+
- **Orquestração:** Docker Compose
- **Log:** Python logging module (custom handlers)

---

## Decisões de Design

### Por que PostgreSQL?
Relacional com suporte a JOINs complexos, integridade referencial e performance com grandes volumes de dados.

### Por que Python?
Excelente para processamento de dados, integração com APIs, bibliotecas maduras (pandas, sqlalchemy).

### Por que Docker?
Isolamento de dependências, reproducibilidade, facilidade de distribuição.

### Por que LEFT JOIN (não INNER)?
Para preservar registros órfãos mesmo sem operadora, evitando perda de dados.

### Por que dois CSVs?
- Um com todos os dados (análise global)
- Um com apenas sinistros (análise focada)

### Por que checkpoint?
Permitir retomada sem reprocessamento em caso de falha.

---

## Cronologia do Desenvolvimento

1. **Fase 1:** Integração com API ANS, download de trimestres
2. **Fase 2:** Extração e processamento de demonstrações contábeis
3. **Fase 3:** Normalização e validação de dados
4. **Fase 4:** Download e processamento de operadoras
5. **Fase 5:** Implementação de checkpoint e recuperação
6. **Fase 6:** Consolidação via LEFT JOIN em banco de dados
7. **Fase 7:** Implementação de logging por sessão
8. **Fase 8:** Geração de dois CSVs (todas + sinistros)
9. **Fase 9:** Packaging em ZIP com logs

---

## Suporte e Contribuição

Para problemas ou melhorias, verifique:
- Arquivo de log da sessão
- Estado do checkpoint
- Logs do container Docker

Todos os erros são registrados com contexto suficiente para diagnóstico.


