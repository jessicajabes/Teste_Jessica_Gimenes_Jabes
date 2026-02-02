"""
Aplicação FastAPI Principal

TRADE-OFF: Escolha do Framework
DECISÃO: FastAPI (Opção B)

JUSTIFICATIVA:

1. PERFORMANCE:
   - Baseado em Starlette (async/await nativo)
   - Um dos frameworks Python mais rápidos
   - Comparável a Node.js e Go em benchmarks
   - Ideal para APIs com múltiplas requisições simultâneas

2. DOCUMENTAÇÃO AUTOMÁTICA:
   - Swagger UI out-of-the-box (/docs)
   - ReDoc automático (/redoc)
   - Reduz significativamente tempo de documentação
   - Facilita testes e integração

3. VALIDAÇÃO DE DADOS:
   - Usa Pydantic para validação automática
   - Type hints Python nativos
   - Erros claros e estruturados
   - Reduz código boilerplate

4. MANUTENIBILIDADE:
   - Código moderno e limpo (Python 3.6+)
   - Separação clara de responsabilidades
   - Dependency injection nativo
   - Fácil de testar (pytest-async)

5. ECOSSISTEMA:
   - Suporte a OpenAPI 3.0
   - Compatível com diversos ORMs
   - Middleware para CORS, autenticação, etc.
   - Comunidade ativa e crescente

QUANDO USAR FLASK (Opção A):
- Projetos muito simples (1-2 rotas)
- Equipe já familiarizada com Flask
- Necessidade de extensões específicas do Flask
- Não requer alta performance assíncrona

COMPARAÇÃO:
FastAPI vs Flask:
- FastAPI: 2-3x mais rápido em benchmarks
- FastAPI: Documentação automática (Flask precisa Flask-RESTX)
- FastAPI: Validação automática (Flask precisa marshmallow)
- Flask: Mais maduro e estável (desde 2010)
- Flask: Mais extensões disponíveis
"""
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from app.config import get_settings
from app.routers import operadoras, estatisticas
from app.database.connection import test_connection

settings = get_settings()

# Criar aplicação FastAPI
app = FastAPI(
    title=settings.api_title,
    version=settings.api_version,
    description=settings.api_description,
    docs_url="/docs",
    redoc_url="/redoc"
)

# Configurar CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Incluir routers
app.include_router(operadoras.router)
app.include_router(estatisticas.router)


@app.on_event("startup")
async def startup_event():
    """Executado ao iniciar a aplicação"""
    print("=" * 60)
    print(f"🚀 {settings.api_title} v{settings.api_version}")
    print("=" * 60)
    
    # Testar conexão com banco
    if test_connection():
        print("✓ Conexão com banco de dados OK")
    else:
        print("✗ Erro na conexão com banco de dados")
    
    print(f"\n📚 Documentação: http://{settings.api_host}:{settings.api_port}/docs")
    print("=" * 60)


@app.get("/", tags=["Health"])
async def root():
    """Endpoint raiz - Health check"""
    return {
        "service": settings.api_title,
        "version": settings.api_version,
        "status": "running",
        "docs": "/docs"
    }


@app.get("/health", tags=["Health"])
async def health_check():
    """Verificação de saúde da API"""
    db_status = test_connection()
    
    return {
        "status": "healthy" if db_status else "unhealthy",
        "database": "connected" if db_status else "disconnected"
    }
