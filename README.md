# Intuitive Care — Consolidação de Demonstrações Contábeis da ANS

## Introdução

**Exercício 1 — Teste de Integração com API Pública** 

O objetivo inicial do exercício era **baixar automaticamente os documentos das 3 últimas demonstrações contábeis disponibilizadas pela API pública da ANS**, extrair os arquivos e **processar apenas aqueles que contivessem informações relacionadas a *“DESPESAS COM EVENTOS/SINISTROS”***.

Durante o desenvolvimento, foram identificados **pontos de atenção e cenários potenciais de inconsistência** nos dados de origem que exigiram **decisões técnicas conscientes e preventivas**, principalmente relacionadas a:

* Ausência de CNPJ e Razão Social nos arquivos de demonstrações contábeis, o que exigiu a realização de JOIN com outra base já no primeiro exercício
* Volume e diversidade de arquivos disponibilizados pela API
* Diferenças e variações de dados entre trimestres distintos
* Necessidade de rastreabilidade, auditoria e recuperação em caso de falha

Este README documenta as decisões tomadas, as dificuldades consideradas e **como cada etapa do processamento foi desenhada para lidar com esses cenários**, mantendo uma abordagem objetiva e sem excesso de abstrações.

---

## Visão Geral da Solução

O sistema:

* É totalmente executado em **containers Docker**
* Integra-se à **API pública da ANS**
* Processa automaticamente arquivos nos formatos **CSV, XLSX e TXT**
* Normaliza, valida e consolida dados contábeis
* Persiste dados em **PostgreSQL** para otimizar joins e auditoria
* Gera arquivos CSV consolidados e logs detalhados por execução

---

## Como Executar o Projeto

### Pré-requisitos

* Docker 20.10+
* Docker Compose 1.29+
* Windows, Linux ou macOS

---

### Passo 1 — Subir os Containers

```bash
docker-compose up -d
```

Isso irá iniciar:

* PostgreSQL
* Container da aplicação de integração

Verifique se estão ativos:

```bash
docker-compose ps
```

---

### Passo 2 — Executar o Script Principal

```bash
docker exec -i intuitive-care-integracao-api \
python /app/1-integracao_api_publica/main.py
```

Tempo médio de execução: **5 a 15 minutos**, dependendo da conexão e do ambiente.

---

### Passo 3 — Verificar Resultados

Os arquivos gerados estarão em:

```
backend/1-integracao_api_publica/consolidados/
```

Conteúdo esperado:

* Arquivos CSV consolidados
* Arquivo ZIP contendo os CSVs e o log da sessão

---

## Tecnologias Utilizadas

* Python 3.9+
* Pandas
* SQLAlchemy
* PostgreSQL 15
* Docker / Docker Compose
* API Pública da ANS

---

## Exercício 1 — Integração com API Pública da ANS

### Objetivo Original

* Baixar automaticamente os arquivos das **3 últimas demonstrações contábeis** disponíveis
* Extrair os arquivos
* Processar apenas aqueles que **contivessem “DESPESAS COM EVENTOS/SINISTROS”**

### Implementação

* Consulta automática à API para identificar os últimos trimestres
* Download dos arquivos ZIP
* Extração automática dos conteúdos
* Validação prévia para verificar se o arquivo contém o dado solicitado antes do processamento
* Suporte aos formatos: **CSV, XLSX e TXT**

---

## Decisão Técnica: Processamento em Memória vs Incremental

Durante o desenvolvimento, foi necessário decidir entre:

### Opção A — Processar tudo em memória

* Join via Pandas
* Maior velocidade inicial

**Limitações:**

* Alto consumo de memória
* Falha implica reiniciar todo o processo
* Pouca rastreabilidade

### Opção B — Processamento Incremental (Escolhida)

* Importação gradual para PostgreSQL
* Join via SQL

**Vantagens:**

* Consumo de memória constante
* Possibilidade de retomar após falhas
* Melhor auditoria
* Mais escalável

**Decisão:** foi adotado o **processamento incremental com banco de dados**, considerando o volume de dados e a necessidade de confiabilidade.

---

## Consolidação dos Dados

### Problema Encontrado

Os arquivos de demonstrações contábeis **não possuíam CNPJ nem Razão Social**, apenas o `registro_da_operadora`.

### Solução

* Download adicional da API:

  * Operadoras ativas
  * Operadoras inativas
* Ambas foram carregadas no banco, pois a operadora pode ter se tornado inativa entre trimestres
* Consolidação realizada via **LEFT JOIN** utilizando `registro_da_operadora`

---

## Arquivos de Saída

Foram gerados **dois arquivos CSV**, visando atender ao enunciado e também permitir auditoria completa:

### 1. CSV com Todos os Dados Processados

* Contém todos os registros válidos encontrados nos arquivos
* Útil para análise global e conferência

### 2. CSV Apenas com Despesas de Eventos/Sinistros

* Contém apenas registros cujo descritivo do plano de contas possui:

  * `DESPESAS COM EVENTOS/SINISTROS`
* Inclui também valores de dedução (quando existentes)

Ambos são compactados em um único arquivo ZIP junto com o log da execução.

---

## Checkpoint e Recuperação

Foi implementado um sistema de **checkpoint**, que registra o progresso após cada trimestre processado.

* Caso o processo seja interrompido ou cancelado
* A próxima execução retoma exatamente do ponto onde parou
* Não há reprocessamento desnecessário

---

## Logging e Rastreabilidade

* Cada execução gera um **arquivo de log próprio**
* Nenhum dado é perdido:

  * Ou ele é processado
  * Ou o motivo da falha fica registrado no log

Isso garante total rastreabilidade e facilita auditorias.

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
