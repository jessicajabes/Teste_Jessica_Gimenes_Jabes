"""
Configuração da aplicação - Núcleo (Core)
"""
import os
from pydantic_settings import BaseSettings
from functools import lru_cache
from dotenv import load_dotenv

load_dotenv()


class Settings(BaseSettings):
    """Configurações da aplicação"""
    
    # Database
    database_url: str = os.getenv(
        "DATABASE_URL",
        "postgresql://jessica:1234@localhost:55432/intuitive_care",
    )
    
    # Cache
    cache_ttl_seconds: int = int(os.getenv("STATS_CACHE_TTL", "300"))
    
    # API
    api_host: str = os.getenv("API_HOST", "0.0.0.0")
    api_port: int = int(os.getenv("API_PORT", "8000"))
    api_title: str = "Intuitive Care - API de Operadoras"
    api_version: str = "1.0.0"
    api_description: str = """
    API RESTful para consulta de operadoras de saúde e despesas.
    
    ## Funcionalidades
    
    * **Operadoras**: Listagem paginada, busca e detalhes
    * **Despesas**: Histórico de despesas por operadora
    * **Estatísticas**: Agregações e análises de dados
    """
    
    # CORS
    cors_origins: list = [
        os.getenv("CORS_ORIGINS", "http://localhost:5173")
    ]
    
    class Config:
        env_file = ".env"


@lru_cache()
def get_settings():
    """Retorna instância única de Settings (cache)"""
    return Settings()
