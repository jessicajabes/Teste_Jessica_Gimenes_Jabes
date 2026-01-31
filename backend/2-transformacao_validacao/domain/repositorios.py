"""
Repositórios de domínio para transformação e validação
"""

from abc import ABC, abstractmethod
from typing import List, Dict
import pandas as pd

class RepositorioCSV(ABC):
    """Interface para leitura de arquivos CSV"""
    
    @abstractmethod
    def ler_arquivo(self, caminho: str) -> pd.DataFrame:
        pass

class RepositorioBanco(ABC):
    """Interface para acesso ao banco de dados"""
    
    @abstractmethod
    def conectar(self) -> bool:
        pass
    
    @abstractmethod
    def desconectar(self):
        pass
    
    @abstractmethod
    def inserir_demonstracoes(self, dados: List[Dict]) -> int:
        pass
    
    @abstractmethod
    def listar_demonstracoes(self, filtros: Dict = None) -> pd.DataFrame:
        pass
