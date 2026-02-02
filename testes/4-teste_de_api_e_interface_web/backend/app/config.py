"""
Configuração da aplicação
"""
from pydantic_settings import BaseSettings
from functools import lru_cache


class Settings(BaseSettings):
    """Configurações da aplicação"""
    
    # Database
    database_url: str = "postgresql://jessica:password@localhost:55432/intuitive_care"
    
    # Cache
    cache_ttl_seconds: int = 300  # 5 minutos
    
    # API
    api_host: str = "0.0.0.0"
    api_port: int = 8000
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
    cors_origins: list = ["http://localhost:3000", "http://localhost:5173", "http://localhost:8080"]
    
    class Config:
        env_file = ".env"
        case_sensitive = False


@lru_cache()
def get_settings() -> Settings:
    """Retorna instância singleton das configurações"""
    return Settings()
