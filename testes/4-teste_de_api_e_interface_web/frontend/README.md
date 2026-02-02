# Frontend - Sistema de Operadoras

Interface web construída com Vue.js 3 para visualização de operadoras de saúde.

## 🚀 Início Rápido

```bash
# Instalar dependências
npm install

# Configurar API
cp .env .env.local
# Editar .env.local se necessário

# Executar em desenvolvimento
npm run dev

# Build para produção
npm run build
npm run preview
```

Aplicação disponível em: http://localhost:5173

## ✨ Funcionalidades

- ✅ Listagem paginada de operadoras
- ✅ Busca por razão social ou CNPJ (com debounce)
- ✅ Dashboard com estatísticas agregadas
- ✅ Gráfico de despesas por UF (Chart.js)
- ✅ Top 5 operadoras com maiores despesas
- ✅ Página de detalhes com histórico de despesas
- ✅ Design responsivo
- ✅ Tratamento de erros e loading states

## 🏗️ Estrutura

```
src/
├── components/           # Componentes reutilizáveis
│   ├── TabelaOperadoras.vue
│   ├── Paginacao.vue
│   ├── GraficoDespesasUf.vue
│   └── TabelaTop5.vue
├── views/               # Páginas
│   ├── Home.vue         # Dashboard
│   └── Detalhes.vue     # Detalhes da operadora
├── services/            # API client
│   ├── api.js           # Axios config + interceptors
│   └── operadoras.js    # Funções de API
├── router/              # Vue Router config
├── App.vue              # Componente raiz
├── main.js              # Entry point
└── style.css            # Estilos globais
```

## 🎨 Tecnologias

- **Vue.js 3** - Framework reativo
- **Vue Router 4** - Roteamento SPA
- **Vite** - Build tool
- **Axios** - HTTP client
- **Chart.js** - Gráficos
- **vue-chartjs** - Wrapper Vue para Chart.js

## 📱 Responsividade

A interface se adapta a diferentes tamanhos de tela:
- Desktop: Layout completo com sidebar de estatísticas
- Tablet: Grid adaptável
- Mobile: Cards empilhados, tabelas com scroll horizontal

## 🔧 Configuração

### Variáveis de Ambiente (.env)

```env
VITE_API_URL=http://localhost:8000
```

### Configuração do Vite (vite.config.js)

```javascript
export default {
  server: {
    port: 5173,
    host: true
  }
}
```

## 🧪 Scripts Disponíveis

```bash
# Desenvolvimento
npm run dev

# Build produção
npm run build

# Preview build
npm run preview

# Linting
npm run lint
```

## 🚀 Deploy

### Build estático

```bash
npm run build
# Arquivos em: dist/
```

Pode ser servido por:
- Nginx
- Apache
- Vercel
- Netlify
- GitHub Pages

### Docker

```bash
docker build -t frontend-operadoras .
docker run -p 5173:5173 frontend-operadoras
```

## 🎯 Decisões Técnicas

### Composition API
Uso da Composition API do Vue 3 para melhor organização e reusabilidade.

### Debounce em Busca
Aguarda 500ms após o usuário parar de digitar para fazer a requisição.

### Paginação Server-side
Carrega apenas os dados necessários, reduzindo payload.

### Interceptors Axios
Tratamento centralizado de erros HTTP.

### Chart.js
Biblioteca leve e flexível para gráficos.

## 📝 Melhorias Futuras

- [ ] Testes unitários (Vitest)
- [ ] Testes E2E (Playwright)
- [ ] PWA support
- [ ] Modo escuro
- [ ] Internacionalização (i18n)
- [ ] Cache de requisições
