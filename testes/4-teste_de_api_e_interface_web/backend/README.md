# Módulo 4 - Teste de API e Interface Web (Backend)

## Decisões Técnicas (Trade-offs)

### 4.2.1 Escolha do Framework
**Escolha:** FastAPI

**Justificativa:**
- **Complexidade do projeto:** Rotas simples e bem definidas. FastAPI oferece validação automática, tipagem e documentação Swagger sem esforço adicional.
- **Performance esperada:** FastAPI é altamente performático por usar ASGI e async-friendly.
- **Manutenção:** Tipos claros com Pydantic reduzem erros e facilitam evolução do código.

### 4.2.2 Estratégia de Paginação
**Escolha:** Offset-based (`page` + `limit`)

**Justificativa:**
- Volume atual de dados é moderado (~4k operadoras) e atualizações são pouco frequentes.
- Implementação simples e fácil para o frontend.
- Para volumes muito grandes, keyset seria melhor, mas aqui o custo não compensa.

### 4.2.3 Cache vs Queries Diretas (estatísticas)
**Escolha:** Cache em memória por X minutos (padrão: 10 min)

**Justificativa:**
- Estatísticas são pesadas e não mudam com frequência.
- Cache reduz carga no banco e melhora latência.
- Consistência eventual é aceitável para visão analítica.

### 4.2.4 Estrutura de Resposta da API
**Escolha:** Dados + metadados (`{ data, total, page, limit }`)

**Justificativa:**
- O frontend precisa do `total` para renderizar paginação.
- Evita múltiplas chamadas para saber tamanho total.

## Como executar

```bash
# instalar dependências
pip install -r requirements.txt

# executar API
uvicorn app:app --host 0.0.0.0 --port 8000
```

A documentação interativa ficará em:
- http://localhost:8000/docs

## Rotas Disponíveis

- `GET /api/operadoras?page=1&limit=10&q=texto`
- `GET /api/operadoras/{cnpj}`
- `GET /api/operadoras/{cnpj}/despesas`
- `GET /api/estatisticas`
