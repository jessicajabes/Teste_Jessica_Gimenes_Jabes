from abc import ABC, abstractmethod
from typing import List
from domain.entidades import Trimestre, Arquivo

class RepositorioAPI(ABC):
    @abstractmethod
    def obter_anos_disponiveis(self) -> List[int]:
        pass
    
    @abstractmethod
    def obter_trimestres_do_ano(self, ano: int) -> List[str]:
        pass
    
    @abstractmethod
    def obter_arquivos_do_trimestre(self, trimestre: Trimestre) -> List[str]:
        pass
    
    @abstractmethod
    def baixar_arquivo(self, arquivo: Arquivo, destino: str) -> bool:
        pass

class RepositorioArquivo(ABC):
    @abstractmethod
    def extrair_zips(self, diretorio: str) -> List[str]:
        pass
    
    @abstractmethod
    def encontrar_arquivos_dados(self, diretorio: str) -> dict:
        pass
