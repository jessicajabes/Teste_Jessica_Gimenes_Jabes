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

## Exercício 1 — Integração com API Pública

O que faz:
- Baixa os 3 últimos trimestres da ANS
- Extrai ZIPs e filtra despesas de eventos/sinistros
- Normaliza formatos CSV/TXT/XLSX
- Gera `consolidado_despesas.zip`

Trade-off técnico (memória vs incremental):
- Escolha: processamento incremental com PostgreSQL.
- Motivo: menor uso de memória, retomada após falhas e melhor auditoria.

Tratamento de inconsistências:
- Razão social/CNPJ ausentes: JOIN com cadastro de operadoras.
- Valores zerados/negativos: mantidos para rastreabilidade.
- Trimestres inconsistentes: normalizados no pipeline.

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
