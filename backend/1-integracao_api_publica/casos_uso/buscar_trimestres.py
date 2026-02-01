from typing import List, Optional
import re

from config import API_BASE_URL
from domain.entidades import Trimestre
from domain.repositorios import RepositorioAPI
from infraestrutura.repositorio_api_http import RepositorioAPIHTTP

class BuscarUltimosTrimestres:
    def __init__(self, repositorio: Optional[RepositorioAPI] = None, quantidade: int = 3):
        if repositorio is None:
            self.repositorio = RepositorioAPIHTTP(API_BASE_URL)
            self._repositorio_interno = True
        else:
            self.repositorio = repositorio
            self._repositorio_interno = False
        self.quantidade = quantidade
    
    def executar(self) -> List[Trimestre]:
        try:
            anos = self.repositorio.obter_anos_disponiveis()
            if not anos:
                return []
            
            anos_ordenados = sorted(anos, reverse=True)
            trimestres_encontrados = []
            ordem_trimestres = [4, 3, 2, 1]
            
            for ano in anos_ordenados:
                trimestres_ano = self.repositorio.obter_trimestres_do_ano(ano)
                
                for numero_trimestre in ordem_trimestres:
                    trimestre_procurado = f"{numero_trimestre}T"
                    
                    encontrado = False
                    for trimestre_str in trimestres_ano:
                        numero = self._extrair_numero_trimestre(trimestre_str)
                        if numero == numero_trimestre:
                            trimestre = Trimestre(ano=ano, numero=numero)
                            trimestres_encontrados.append(trimestre)
                            encontrado = True
                            break
                    
                    if encontrado and len(trimestres_encontrados) >= self.quantidade:
                        return trimestres_encontrados[:self.quantidade]
            
            return trimestres_encontrados[:self.quantidade]
        finally:
            if self._repositorio_interno:
                self.repositorio.fechar()
    
    @staticmethod
    def _extrair_numero_trimestre(trimestre: str) -> int:
        match = re.search(r'(\d+)', trimestre)
        return int(match.group(1)) if match else 0
