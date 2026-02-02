# Backend - Sistema de Análise de Dados ANS

Módulos de backend organizados por funcionalidade.

## Estrutura

### 1-integracao_api_publica
Primeira funcionalidade do sistema: integração com a API pública da ANS para download e consolidação de dados.

**Responsabilidades:**
- Buscar trimestres disponíveis na API
- Baixar arquivos ZIP
- Extrair e processar dados
- Gerar CSVs consolidados
- Inserir dados no banco PostgreSQL

**Arquivos Gerados:** `downloads/Integracao/`

### 2-transformacao_validacao
Segunda funcionalidade do sistema: leitura, transformação e validação dos dados consolidados.

**Responsabilidades:**
- Ler os CSVs gerados pela integração
- Validar integridade dos dados
- Realizar análise estatística
- Detectar inconsistências
- Gerar relatórios de qualidade

**Arquivos Lidos:** `./backend/downloads/Integracao/consolidado_*.csv`

## Execução

Cada módulo pode ser executado independentemente:

```bash
# Módulo 1
cd 1-integracao_api_publica
python main.py

# Módulo 2
cd 2-transformacao_validacao
python main.py
```

Ou através do Main.py na raiz do projeto para executar ambos em sequência.

## Docker

Cada módulo possui seu próprio Dockerfile e pode ser executado via docker-compose.
