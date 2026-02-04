
from typing import Optional, Dict, Any
from app.repositories.operadora_repository import (
    listar_operadoras as repo_listar_operadoras,
    obter_operadora_por_cnpj as repo_obter_operadora,
    obter_despesas_por_cnpj as repo_obter_despesas,
)

class OperadoraService:
   
    @staticmethod
    def listar_operadoras(
        page: int = 1, 
        limit: int = 10, 
        search: Optional[str] = None
    ) -> Dict[str, Any]:

        return repo_listar_operadoras(page, limit, search)
    
    @staticmethod
    def obter_operadora_por_cnpj(cnpj: str) -> Optional[Dict[str, Any]]:

        return repo_obter_operadora(cnpj)
    
    @staticmethod
    def obter_despesas_operadora(cnpj: str) -> list[Dict[str, Any]]:

        return repo_obter_despesas(cnpj)

