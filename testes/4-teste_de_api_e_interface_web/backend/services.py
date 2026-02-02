"""Serviços auxiliares (cache de estatísticas)"""
import time
from typing import Dict, Any
from config import STATS_CACHE_TTL
from repositories import obter_estatisticas


_stats_cache: Dict[str, Any] = {"value": None, "expires_at": 0}


def get_estatisticas_cached() -> Dict[str, Any]:
    now = time.time()
    if _stats_cache["value"] is not None and now < _stats_cache["expires_at"]:
        return _stats_cache["value"]

    value = obter_estatisticas()
    _stats_cache["value"] = value
    _stats_cache["expires_at"] = now + STATS_CACHE_TTL
    return value
