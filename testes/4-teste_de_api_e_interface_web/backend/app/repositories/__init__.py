"""Módulo repositories - acesso ao banco de dados"""
from app.repositories.operadora_repository import (
    listar_operadoras,
    obter_operadora_por_cnpj,
    obter_despesas_por_cnpj,
    obter_estatisticas,
)

__all__ = [
    "listar_operadoras",
    "obter_operadora_por_cnpj",
    "obter_despesas_por_cnpj",
    "obter_estatisticas",
]
