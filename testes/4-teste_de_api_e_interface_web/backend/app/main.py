
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from app.core.config import get_settings
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
    print(f" {settings.api_title} v{settings.api_version}")
    print("=" * 60)
    
    # Testar conexão com banco
    if test_connection():
        print("[OK] Conexão com banco de dados OK")
    else:
        print("[ERRO] Erro na conexão com banco de dados")
    
    print(f"\n Documentação: http://{settings.api_host}:{settings.api_port}/docs")
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
