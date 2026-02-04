#!/usr/bin/env python3
"""
Script para executar a API FastAPI

OPÇÕES:
- uvicorn app.main:app --reload          # Modo desenvolvimento com hot reload
- uvicorn app.main:app --host 0.0.0.0    # Modo produção em 0.0.0.0
"""
import uvicorn
import sys
from app.core.config import get_settings

if __name__ == "__main__":
    settings = get_settings()
    
    # Modo desenvolvimento (com reload) se não houver argumentos
    # Modo produção se houver argumentos específicos
    if len(sys.argv) == 1:
        # Desenvolvimento
        uvicorn.run(
            "app.main:app",
            host=settings.api_host,
            port=settings.api_port,
            reload=True,
            log_level="info"
        )
    else:
        # Passar argumentos direto para uvicorn
        uvicorn.run(
            "app.main:app",
            host=settings.api_host,
            port=settings.api_port,
            reload=False,
            log_level="info"
        )
