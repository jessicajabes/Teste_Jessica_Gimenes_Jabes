"""Caso de Uso: Buscar Trimestres Disponíveis na API ANS."""

from typing import List, Optional
import re

from config import API_BASE_URL
from domain.entidades import Trimestre
from domain.repositorios import RepositorioAPI
from infraestrutura.cliente_api_ans import ClienteAPIANS


class BuscarTrimestresDisponiveis:
    """Busca os últimos N trimestres disponíveis na API ANS.
    
    Regras:
        - Busca os trimestres mais recentes primeiro (ordem decrescente)
        - Retorna quantidade especificada (padrão: 3)
        - Valida existência dos trimestres na API
    """
    
    def __init__(self, repositorio: Optional[RepositorioAPI] = None, quantidade: int = 3):
        """Inicializa busca de trimestres.
        
        Args:
            repositorio: Cliente da API (opcional, cria um novo se None)
            quantidade: Número de trimestres a buscar (padrão: 3)
        """
        if repositorio is None:
            self.repositorio = ClienteAPIANS(API_BASE_URL)
            self._repositorio_interno = True
        else:
            self.repositorio = repositorio
            self._repositorio_interno = False
        self.quantidade = quantidade
    
    def executar(self) -> List[Trimestre]:
        """Busca os últimos N trimestres disponíveis.
        
        Returns:
            Lista de Trimestre ordenada do mais recente ao mais antigo
        """
        try:
            # Buscar anos disponíveis na API
            anos = self.repositorio.obter_anos_disponiveis()
            if not anos:
                return []
            
            # Ordenar anos (mais recente primeiro)
            anos_ordenados = sorted(anos, reverse=True)
            trimestres_encontrados = []
            ordem_trimestres = [4, 3, 2, 1]  # 4T, 3T, 2T, 1T
            
            # Buscar trimestres em cada ano
            for ano in anos_ordenados:
                trimestres_ano = self.repositorio.obter_trimestres_do_ano(ano)
                
                # Verificar cada trimestre (do mais recente ao mais antigo)
                for numero_trimestre in ordem_trimestres:
                    trimestre_procurado = f"{numero_trimestre}T"
                    
                    # Verificar se trimestre existe
                    encontrado = False
                    for trimestre_str in trimestres_ano:
                        numero = self._extrair_numero_trimestre(trimestre_str)
                        if numero == numero_trimestre:
                            trimestre = Trimestre(ano=ano, numero=numero)
                            trimestres_encontrados.append(trimestre)
                            encontrado = True
                            break
                    
                    # Parar se já encontrou quantidade desejada
                    if encontrado and len(trimestres_encontrados) >= self.quantidade:
                        return trimestres_encontrados[:self.quantidade]
            
            return trimestres_encontrados[:self.quantidade]
        
        finally:
            # Fechar conexão se foi criada internamente
            if self._repositorio_interno:
                self.repositorio.fechar()
    
    def _extrair_numero_trimestre(self, texto: str) -> Optional[int]:
        """Extrai número do trimestre de string (ex: '1T2023' → 1).
        
        Args:
            texto: String contendo informação do trimestre
            
        Returns:
            Número do trimestre (1-4) ou None se não encontrado
        """
        match = re.search(r'(\d)T', texto)
        return int(match.group(1)) if match else None
