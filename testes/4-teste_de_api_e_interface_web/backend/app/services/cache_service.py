"""
Serviço de Cache com TTL
TRADE-OFF: Cache com TTL vs Queries Diretas

DECISÃO: Cache com TTL (Opção B)

JUSTIFICATIVA:
- Os dados de estatísticas são custosos para calcular (agregações complexas)
- Os dados do banco não mudam frequentemente (atualizações diárias/semanais)
- TTL de 5 minutos oferece bom balance entre performance e consistência
- Mais simples que pré-calcular e armazenar em tabela (Opção C)
- Mais performático que calcular sempre (Opção A)

IMPLEMENTAÇÃO:
- Usa cachetools.TTLCache para armazenamento em memória
- Cache por chave (permite invalidação seletiva)
- Ideal para APIs com múltiplas requisições simultâneas

LIMITAÇÕES:
- Cache em memória não persiste entre reinicializações
- Não compartilhado entre múltiplas instâncias (usar Redis em produção)
"""
from cachetools import TTLCache
from typing import Optional, Any
from app.config import get_settings

settings = get_settings()

# Cache global com TTL
_cache = TTLCache(maxsize=100, ttl=settings.cache_ttl_seconds)


def get_cached(key: str) -> Optional[Any]:
    """Recupera valor do cache"""
    return _cache.get(key)


def set_cache(key: str, value: Any) -> None:
    """Armazena valor no cache"""
    _cache[key] = value


def clear_cache(key: Optional[str] = None) -> None:
    """
    Limpa cache
    Se key for None, limpa todo o cache
    """
    if key:
        _cache.pop(key, None)
    else:
        _cache.clear()
