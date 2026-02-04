"""
Configuração da aplicação - Re-exportação de app.core.config
Mantido por compatibilidade - usar app.core.config diretamente
"""
from app.core.config import Settings, get_settings

__all__ = ["Settings", "get_settings"]
