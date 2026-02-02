# Item 1: Integração com API Pública ANS

## Objetivo

Integrar dados da API pública da ANS (Agência Nacional de Saúde Suplementar), baixando informações sobre operadoras e suas despesas assistenciais, consolidando em arquivos CSV para processamento posterior.

## Arquitetura Implementada

### Clean Architecture + Domain-Driven Design (DDD)

```
1-integracao_api_publica/
├── casos_uso/              # Application Layer - Orquestração
│   ├── carregar_operadoras.py
│   ├── buscar_trimestres.py
│   ├── baixar_arquivos.py
│   ├── carregar_dados_banco.py
│   └── gerar_arquivos_consolidados.py
├── domain/                 # Domain Layer - Regras de negócio
│   ├── entidades.py        # Operadora, Trimestre, DadoDespesa
│   └── repositorios.py     # Interfaces (contratos)
├── infraestrutura/         # Infrastructure Layer - Implementações
│   ├── repositorio_api_http.py
│   ├── repositorio_operadoras.py
│   ├── repositorio_arquivo_local.py
│   ├── repositorio_banco_dados.py
│   ├── processador_em_lotes.py
│   ├── gerenciador_checkpoint.py
│   └── logger.py
└── main.py                 # Entry point
```

## Decisões Técnicas e Trade-offs

### 1. **Clean Architecture vs Script Monolítico**

#### Escolhido: Clean Architecture

**Justificativa:**
- **Testabilidade**: Cada camada pode ser testada isoladamente
- **Manutenibilidade**: Mudanças em infraestrutura não afetam regras de negócio
- **Escalabilidade**: Fácil adicionar novos casos de uso (ex: carregar dados de outras APIs)
- **Separação de responsabilidades**: Domain não depende de frameworks

**Trade-offs:**
- **Complexidade inicial maior**: ~15 arquivos vs 1 script monolítico
- **Curva de aprendizado**: Requer entendimento de DDD
- **ROI em manutenção**: Compensa após 3+ meses de desenvolvimento

**Alternativa descartada:** Script monolítico
```python
# Seria mais simples inicialmente:
def main():
    operadoras = requests.get(API_URL).json()
    for op in operadoras:
        dados = requests.get(f"{API_URL}/{op['reg_ans']}").json()
        salvar_csv(dados)
```

### 2. **Sistema de Checkpoints vs Re-download Completo**

#### Escolhido: Checkpoints com resumo

**Justificativa:**
- API ANS é **lenta** (~2-5s por operadora)
- ~700 operadoras × 4 trimestres = **~2.800 requisições**
- Tempo total sem checkpoint: **2-4 horas**
- Com checkpoint: Apenas dados novos/faltantes

**Implementação:**
```python
# infraestrutura/gerenciador_checkpoint.py
class GerenciadorCheckpoint:
    def salvar_progresso(self, operadora_id, trimestre):
        """Marca como processado com sucesso"""
        
    def foi_processado(self, operadora_id, trimestre) -> bool:
        """Verifica se já foi baixado"""
```

**Trade-offs:**
- **Resiliência**: Interrupção não perde trabalho
- **Re-execuções rápidas**: Pula dados já baixados
- **Complexidade**: +150 linhas de código
- **Espaço em disco**: Arquivo checkpoint.json (~50KB)

**Alternativa descartada:** Download completo sempre
- Simples, mas **inviável** para produção (4h por execução)

### 3. **Processamento em Lotes (Batch) vs Paralelo**

#### Escolhido: Batch Sequencial com Rate Limiting

**Justificativa:**
- API ANS tem **rate limit implícito** (muitas requisições paralelas causam timeout)
- **Boa cidadania**: Não sobrecarregar servidor público
- Checkpoint garante que interrupções sejam recuperáveis

**Implementação:**
```python
# infraestrutura/processador_em_lotes.py
class ProcessadorEmLotes:
    def processar(self, itens, tamanho_lote=50):
        for lote in self._dividir_em_lotes(itens, tamanho_lote):
            for item in lote:
                self._processar_item(item)
            time.sleep(1)  # Pausa entre lotes
```

**Trade-offs:**
- **Estabilidade**: Sem erros de rate limit
- **Simples**: Sem complexidade de threading/async
- **Performance**: ~2-3h para carga completa
- **Subutilização**: CPU ociosa esperando I/O

**Alternativa descartada:** AsyncIO com ThreadPoolExecutor
```python
# Mais rápido, mas:
async with aiohttp.ClientSession() as session:
    tasks = [fetch(session, url) for url in urls]
    await asyncio.gather(*tasks)
# Problema: Rate limit da API + complexidade
```

**Quando reconsiderar:** Se ANS publicar API com rate limit oficial (ex: 100 req/s)

### 4. **Validação de Dados: No Download vs Pós-processamento**

#### Escolhido: Validação Básica no Download + Completa no Item 2

**Justificativa:**
- **Responsabilidade única**: Item 1 foca em **extrair** dados
- Validações complexas (duplicatas, regras de negócio) ficam no Item 2
- Apenas valida estrutura mínima (JSON válido, campos obrigatórios existem)

**Implementação:**
```python
# casos_uso/baixar_arquivos.py
def _validar_resposta_api(self, dados: dict) -> bool:
    """Valida apenas estrutura básica"""
    return (
        dados is not None and
        'reg_ans' in dados and
        'valor_despesas' in dados
    )
```

**Trade-offs:**
- **Simplicidade**: Item 1 fica focado
- **Flexibilidade**: Mudanças em validação não afetam extração
- **Dados inválidos salvos**: CSV pode ter problemas (tratados no Item 2)

**Alternativa descartada:** Validação completa no download
- Violaria Single Responsibility Principle
- Dificultaria evolução independente dos módulos

### 5. **Formato de Saída: CSV vs Parquet vs JSON**

#### Escolhido: CSV

**Justificativa:**
- **Interoperabilidade**: PostgreSQL COPY aceita CSV nativamente
- **Legibilidade**: Humanos conseguem inspecionar
- **Ferramentas**: Excel, pandas, R leem facilmente
- **Requisito do teste**: Especificação pede CSV

**Trade-offs:**
- **Compatibilidade**: Universal
- **Simplicidade**: `csv.DictWriter` nativo do Python
- **Performance**: Parsing mais lento que Parquet
- **Tipagem**: Tudo vira string (conversão no Item 3)
- **Tamanho**: ~30% maior que Parquet comprimido

**Comparativo:**

| Formato | Tamanho | Velocidade Leitura | Tipagem | Ferramentas |
|---------|---------|-------------------|---------|-------------|
| CSV | 10MB | 1x | Nao | 5/5 |
| Parquet | 3MB | 5x | Sim | 4/5 |
| JSON | 15MB | 0.5x | Atenção | 3/5 |

**Quando reconsiderar:** Se volume crescer para >1GB (considerar Parquet)

### 6. **Logging: Arquivo vs Console vs Sistema Centralizado**

#### Escolhido: Arquivo Rotativo + Console

**Implementação:**
```python
# infraestrutura/logger.py
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        RotatingFileHandler('logs/integracao.log', maxBytes=10MB, backupCount=5),
        StreamHandler()  # Console
    ]
)
```

**Justificativa:**
- **Arquivo**: Auditoria e debugging posterior
- **Console**: Feedback imediato durante execução
- **Rotação**: Evita log infinito (mantém últimos 50MB)

**Trade-offs:**
- **Simplicidade**: Nativo do Python
- **Suficiente**: Para volume de ~700 operadoras
- **Não centralizado**: Em produção, usar ELK/Datadog
- **Sem alertas**: Falhas não notificam automaticamente

**Alternativa para produção:** Structured logging + ELK
```python
import structlog
logger = structlog.get_logger()
logger.info("operadora_processada", reg_ans="12345", trimestre="2024Q1")
```

## Métricas de Performance

### Carga Completa (700 operadoras × 4 trimestres)

| Métrica | Valor |
|---------|-------|
| Tempo total | ~2.5h |
| Requisições/seg | ~0.3 |
| Dados baixados | ~15MB (comprimido) |
| CSVs gerados | 4 arquivos |
| Checkpoint overhead | <1% tempo total |

### Carga Incremental (apenas novos trimestres)

| Métrica | Valor |
|---------|-------|
| Tempo | ~15-30min |
| Operadoras puladas | ~680 (já processadas) |
| Speedup | 5-10x |

## Melhorias Futuras

### Curto Prazo
1. **Retry com exponential backoff** (já implementado)
2. **Métricas em tempo real** (Prometheus)
3. **Notificações de erro** (Slack/Email)

### Longo Prazo
1. **AsyncIO + aiohttp** (se API ANS melhorar)
2. **Cache Redis** para operadoras frequentes
3. **Particionamento** por ano (se volume crescer 10x)

## Conclusão

A arquitetura escolhida prioriza:
- **Manutenibilidade** sobre performance extrema
- **Resiliência** (checkpoints, retry)
- **Simplicidade** (Python nativo, sem frameworks pesados)

**Trade-off principal:** Complexidade arquitetural (+15 arquivos) em troca de código testável, escalável e fácil de evoluir.

**ROI:** Positivo após 3 meses (tempo economizado em manutenção supera custo inicial de desenvolvimento).
