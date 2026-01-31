from typing import List
from domain.entidades import Trimestre
from domain.repositorios import RepositorioAPI
import re

class BuscarUltimosTrimestres:
    def __init__(self, repositorio: RepositorioAPI, quantidade: int = 3):
        self.repositorio = repositorio
        self.quantidade = quantidade
    
    def executar(self) -> List[Trimestre]:
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
    
    @staticmethod
    def _extrair_numero_trimestre(trimestre: str) -> int:
        match = re.search(r'(\d+)', trimestre)
        return int(match.group(1)) if match else 0
