"""Caso de Uso: Carregar Operadoras.

Orquestra o carregamento de tabelas de operadoras usando o repositório especializado.
"""

import pandas as pd
from typing import Dict

from infraestrutura.repositorio_operadoras import RepositorioOperadoras
from infraestrutura.logger import get_logger

logger = get_logger('CarregarOperadoras')


class CarregarOperadoras:
    """Caso de uso para carregar tabelas de operadoras da ANS."""
    
    def __init__(self):
        self.repositorio = RepositorioOperadoras()
        self.arquivo_ativas = self.repositorio.arquivo_ativas
        self.arquivo_canceladas = self.repositorio.arquivo_canceladas
    
    def executar(self) -> Dict:
        """Executa o carregamento das tabelas de operadoras.
        
        Orquestra o repositório para:
        1. Download das tabelas
        2. Normalização de dados
        3. Armazenamento em disco
        
        Returns:
            Dict com resultado do carregamento
        """
        logger.info("Iniciando carregamento de operadoras")
        resultado = self.repositorio.carregar()
        logger.info("Carregamento de operadoras concluído")
        return resultado
    
    def obter_operadora(self, registro: str) -> Dict:
        """Busca dados de uma operadora específica.
        
        Args:
            registro: Número do registro da operadora
        
        Returns:
            Dict com dados da operadora
        """
        return self.repositorio.obter_operadora(registro)
