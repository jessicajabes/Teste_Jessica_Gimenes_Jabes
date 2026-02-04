
from cachetools import TTLCache
from typing import Optional, Any
from app.core.config import get_settings

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

