"""
Serviço de Estatísticas
Orquestração de repositories com cache
"""
from typing import Dict, Any
from app.repositories.operadora_repository import obter_estatisticas as repo_obter_estatisticas
from app.services.cache_service import get_cached, set_cache


class EstatisticasService:
    """Serviço para estatísticas agregadas"""
    
    CACHE_KEY = "estatisticas_gerais"
    
    @staticmethod
    def obter_estatisticas() -> Dict[str, Any]:

        # Verificar cache primeiro
        cached_result = get_cached(EstatisticasService.CACHE_KEY)
        if cached_result:
            return cached_result
        
        # Buscar do repository (que acessa o banco)
        resultado = repo_obter_estatisticas()
        
        # Armazenar no cache
        set_cache(EstatisticasService.CACHE_KEY, resultado)
        
        return resultado
