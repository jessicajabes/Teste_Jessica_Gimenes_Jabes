# Item 4: API REST e Interface Web

## Como executar

### Docker (API + Frontend)
Executar ambos os containers:

```powershell
docker-compose up -d api_operadoras frontend_operadoras
```

Ou pelo script interativo (selecionar opção 4):

```powershell
powershell -File .\executar_por_teste.ps1
```

### Backend (FastAPI)
1. Abra um terminal em `testes/4-teste_de_api_e_interface_web/backend`
2. Instale as dependências: `pip install -r requirements.txt`
3. Execute a API: `python run.py`

Rotas e docs:
- `http://localhost:8000/docs`

Como visualizar a documentação automática:
- Com a API rodando, acesse os links acima para ver e testar as rotas no navegador.

### Frontend (Vue.js)
1. Abra um terminal em `testes/4-teste_de_api_e_interface_web/frontend`
2. Instale as dependências: `npm install`
3. Configure a API em `.env` (`VITE_API_URL`)
4. Execute: `npm run dev`

Aplicação em `http://localhost:5173`

## Dados utilizados

Escolha: banco do teste 3 (PostgreSQL) acessado via SQLAlchemy.

Prós:
- Agregações e filtros com SQL nas tabelas `operadoras`, `despesas_agregadas` e `despesas_agregadas_c_deducoes`.
- Melhor desempenho para paginação e estatísticas.

Contras:
- Requer banco configurado localmente ou via Docker.
- Leitura direta de CSV não foi implementada neste código.

## Endpoints

| Método | Endpoint | Descrição |
|--------|----------|-----------|
| GET | `/` | Health check raiz |
| GET | `/health` | Status da API e banco |
| GET | `/docs` | Documentação Swagger |
| GET | `/api/operadoras` | Listar operadoras (paginado) |
| GET | `/api/operadoras/{cnpj}` | Detalhes operadora |
| GET | `/api/operadoras/{cnpj}/despesas` | Despesas operadora |
| GET | `/api/estatisticas` | Estatísticas gerais |

## Trade-offs técnicos - Backend

### 4.2.1 Framework
Escolha: FastAPI.

Prós:
- É rápido e já vem com documentação automática das rotas.
- Ajuda a evitar erros simples porque valida os dados de entrada.
- Deixa o código mais organizado para crescer depois.

Contras:
- Tem mais conceitos para aprender do que Flask.
- Tem menos exemplos prontos do que Django REST.

Alternativa considerada: Flask.

Prós do Flask:
- É bem simples de começar e tem muitos exemplos.
- É leve e fácil para projetos pequenos.

Contras do Flask:
- A validação e a documentação precisam ser feitas “na mão”.
- Em projetos que crescem, tende a virar mais código repetido.

Conclusão:
Escolhi FastAPI porque a API tem várias rotas e dados com validação. Ele entrega desempenho melhor e documentação automática, o que facilita manutenção e testes, mesmo sendo um pouco mais difícil no início.

### 4.2.2 Paginação
Escolha: offset-based (`page` + `limit`).
Descrição:
- Usa número da página e quantidade por página. Ex: `page=2&limit=10` pega os itens 11–20.

Prós:
- Simples de implementar e consumir.
- Permite “ir para página X”.

Contras:
- Perde performance com offsets altos.
- Pode sofrer inconsistência em datasets muito mutáveis.

Alternativas consideradas:

Opção B: Cursor-based.
Descrição:
- Usa um “marcador” do último item da página anterior (ex: id ou timestamp) para buscar a próxima página.
Prós:
- Melhor performance em listas grandes e scroll infinito.
Contras:
- Não permite pular direto para uma página específica.

Opção C: Keyset pagination.
Descrição:
- Usa uma coluna ordenável (ex: id, data) e traz itens “maiores/menores” que o último visto.
Prós:
- Performance estável em volumes maiores.
Contras:
- Requer uma coluna ordenável bem definida.
- Não permite “pular” páginas facilmente.

Conclusão:
Escolhi offset-based porque o volume é moderado e as atualizações não são constantes. Isso simplifica o frontend e mantém a navegação por páginas, que é mais fácil para o usuário.

Exemplo na API:
- `GET /api/operadoras?page=1&limit=10`

### 4.2.3 Cache vs queries diretas
Escolha: cache em memória com TTL para `/api/estatisticas`.
Descrição:
- O resultado fica guardado por alguns minutos para evitar repetir a consulta pesada toda hora.

Prós:
- Reduz custo de agregações pesadas.
- Latência menor no dashboard.

Contras:
- Dados ficam desatualizados até o TTL expirar. Como os dados são trimestrais, isso tem baixo impacto prático e pode ser visto como aceitável.
- Cache em memória não escala entre múltiplas instâncias.

Detalhe de implementação:
- TTL configurável via `STATS_CACHE_TTL` (padrão 300s).

Alternativas consideradas:

Opção A: Calcular sempre na hora.
Descrição:
- Executa as agregações no banco a cada chamada.
Prós:
- Dados sempre atualizados.
Contras:
- Mais lento e gera mais carga no banco.

Opção C: Pré-calcular e armazenar em tabela.
Descrição:
- Uma rotina atualiza uma tabela com estatísticas já prontas.
Prós:
- Resposta muito rápida e previsível.
Contras:
- Exige job agendado e mais manutenção.
- Dados ficam desatualizados até a próxima atualização.

Conclusão:
Escolhi cache por X minutos porque as estatísticas são trimestrais e não mudam com frequência. O ganho de performance é alto e a consistência continua adequada para o dashboard, então esta é a melhor escolha para este cenário.

### 4.2.4 Estrutura de resposta
Escolha: dados + metadados (`{ data, total, page, limit }`).
Descrição:
- A resposta vem com a lista e informações de paginação (quantidade total, página atual e limite).

Prós:
- Frontend calcula paginação sem chamadas extras.
- Facilita UX com total de registros.

Contras:
- Resposta levemente maior.

Alternativas consideradas:

Opção A: Apenas os dados (`[{...}, {...}]`).
Prós:
- Resposta menor e mais simples.
Contras:
- O frontend precisa de outra chamada para saber o total.
- Paginação fica mais difícil de montar.

Conclusão:
Escolhi dados + metadados porque deixa o frontend mais simples e evita chamadas extras, o que melhora a experiência do usuário.

### CORS
Foi utilizado CORS com origens configuráveis.

Prós:
- Evita acesso indevido por origens não autorizadas.

Contras:
- Exige manutenção da lista de origens ao mudar ambientes.

Detalhe de implementação:
- `allow_origins` vem de `CORS_ORIGINS` (padrão `http://localhost:5173`).

## Trade-offs técnicos - Frontend

### 4.3.1 Busca/Filtro
Escolha: busca no servidor.
Descrição:
- O frontend envia o termo de busca e a API filtra no banco.

Prós:
- Menor payload e melhor performance em datasets médios/grandes.
- Resultados consistentes com a fonte de dados.

Contras:
- Depende de latência de rede.
- Requer debounce para evitar excesso de chamadas.

Alternativas consideradas:

Opção B: Busca no cliente.
Descrição:
- Carrega todos os dados e filtra no navegador.
Prós:
- Resposta imediata ao digitar.
Contras:
- Fica pesada quando há muitas operadoras.

Opção C: Híbrido.
Descrição:
- Usa busca no servidor e mantém cache local de páginas já visitadas.
Prós:
- Reduz chamadas repetidas.
Contras:
- Mais complexidade no frontend.

Conclusão:
Como o volume de operadoras é grande para carregar tudo no navegador, a busca no servidor é a melhor escolha. Ela mantém a aplicação leve e com resultados consistentes, mesmo que o usuário tenha uma pequena espera de rede.

### 4.3.2 Gerenciamento de estado
Escolha: props/events + serviços de API (sem store global).
Descrição:
- Os dados ficam nos componentes e são passados por props, enquanto as chamadas ficam nos services.

Prós:
- Menor complexidade para app pequeno.
- Menos dependências.

Contras:
- Pode exigir prop drilling se a app crescer.

Alternativas consideradas:

Opção B: Vuex/Pinia.
Descrição:
- Um store global centraliza o estado da aplicação.
Prós:
- Facilita compartilhar estado entre várias telas.
Contras:
- Mais configuração e código para um app simples.

Opção C: Composables (Vue 3).
Descrição:
- Funções reutilizáveis para concentrar lógica e estado.
Prós:
- Reuso de lógica com menos boilerplate que store.
Contras:
- Ainda exige organização extra e pode virar complexidade desnecessária.

Conclusão:
Como o app é simples e o estado não precisa ser global, props/events + services resolvem bem. Se o projeto crescer, um store ou composables pode ser adotado.

### 4.3.3 Performance da tabela
Escolha: paginação server-side e limite de registros por página.
Descrição:
- A tabela não carrega tudo de uma vez. A cada página, o frontend pede à API só um bloco (ex: 10 registros), usando `page` e `limit`.

Prós:
- Evita renderização de grandes volumes no cliente.
- UX previsível com páginas.

Contras:
- Não permite scroll infinito por padrão.
- Requer chamadas adicionais ao trocar página.

Conclusão:
Essa abordagem mantém o navegador leve e rápido, mesmo com muitas operadoras.

### 4.3.4 Erros e loading
Estratégia:
- Loading states durante requisições nas páginas Home e Detalhes.
- Interceptor do Axios mapeia timeout, erro de rede, 404 e 500.
- Mensagem genérica para falhas inesperadas quando não há detalhe do backend.
- Estado vazio quando não há resultados.

Detalhamento:

- Erros de rede/API:
	- Timeout: mensagem clara sobre demora do servidor.
	- Sem conexão: mensagem de erro de rede.
	- 404/500: mensagens específicas por status.
	- Outros: mensagem genérica quando não há detalhe do backend.

- Estados de loading:
	- Exibido no carregamento da lista de operadoras e na tela de detalhes.
	- Evita que o usuário ache que a tela “travou”.

- Dados vazios:
	- Mensagem “Nenhuma operadora encontrada” quando a busca não retorna dados.
	- No detalhe, mostra que não há despesas quando a lista vem vazia.

Prós:
- Melhora UX e reduz frustração.
- Ajuda a diagnosticar falhas comuns.

Contras:
- Mensagens específicas exigem mapeamento de erros.

Análise crítica:
- Usei mensagens específicas para erros comuns porque isso orienta o usuário (ex.: falta de internet).
- Para erros inesperados, a mensagem genérica evita expor detalhes técnicos e mantém a UX simples.

## Documentação

Postman: coleção em `backend/postman_collection.json` com exemplos de requisição e resposta.

## Observação

As rotas do backend atendem às tarefas solicitadas, incluindo paginação, detalhes por CNPJ, histórico de despesas e estatísticas agregadas.

## Conclusão

A arquitetura API + SPA segue o que foi implementado no projeto:
- Backend em FastAPI com rotas e documentação automática.
- Frontend em Vue.js consumindo a API.
- Paginação, cache e tratamento de erros conforme descrito acima.

Trade-off principal:
- Duas aplicações (API + Frontend) aumentam a complexidade de deploy, mas deixam responsabilidades separadas.

Observação final:
- SSR não foi implementado por questão de tempo e porque não estava no escopo. Fica como ideia de melhoria futura.
