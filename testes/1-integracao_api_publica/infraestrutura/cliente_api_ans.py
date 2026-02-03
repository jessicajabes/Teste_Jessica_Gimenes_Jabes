"""Cliente HTTP para comunicação com a API da ANS.

Responsável por todas as operações de rede com a API ANS:
- Buscar anos disponíveis
- Buscar trimestres de um ano
- Listar arquivos de um trimestre
- Baixar arquivos ZIP
"""

import requests
import re
import os
from typing import List

from domain.entidades import Trimestre, Arquivo
from domain.repositorios import RepositorioAPI


class ClienteAPIANS(RepositorioAPI):
    """Cliente HTTP para interagir com a API da ANS."""
    
    def __init__(self, url_base: str):
        """Inicializa cliente da API.
        
        Args:
            url_base: URL base da API ANS
        """
        self.url_base = url_base
        self.sessao = requests.Session()
        self.sessao.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
    
    def obter_anos_disponiveis(self) -> List[int]:
        """Busca lista de anos disponíveis na API.
        
        Returns:
            Lista de anos (ex: [2023, 2022, 2021])
        """
        url = f"{self.url_base}/demonstracoes_contabeis/"
        
        try:
            resposta = self.sessao.get(url, timeout=10)
            resposta.raise_for_status()
            
            # Extrair links de pastas (que são anos)
            padrao = r'href="([^"]+/)"'
            matches = re.findall(padrao, resposta.text)
            
            anos = []
            for match in matches:
                pasta = match.rstrip('/')
                if pasta.isdigit():
                    anos.append(int(pasta))
            
            return sorted(anos, reverse=True)
        
        except requests.exceptions.RequestException as e:
            print(f" Erro ao buscar anos: {e}")
            return []
    
    def obter_trimestres_do_ano(self, ano: int) -> List[str]:
        """Busca trimestres disponíveis para um ano.
        
        Args:
            ano: Ano a buscar (ex: 2023)
            
        Returns:
            Lista de trimestres encontrados (ex: ['1T', '2T', '3T', '4T'])
        """
        try:
            print(f"  Buscando trimestres disponíveis em {ano}...", end=" ", flush=True)
            
            # Buscar todos os arquivos do ano recursivamente
            arquivos = []
            self._buscar_recursivo(f"{ano}", "", arquivos)
            
            # Extrair números dos trimestres dos nomes dos arquivos
            trimestres_encontrados = set()
            for caminho in arquivos:
                match = re.search(r'(\d)[tT]', caminho)
                if match:
                    trimestres_encontrados.add(f"{match.group(1)}T")
            
            trimestres_validos = sorted(trimestres_encontrados)
            print(f"Encontrados: {trimestres_validos}", flush=True)
            return trimestres_validos
        
        except requests.exceptions.RequestException as e:
            print(f" Erro: {e}")
            return []
    
    def obter_arquivos_do_trimestre(self, trimestre: Trimestre) -> List[str]:
        """Lista arquivos disponíveis para um trimestre.
        
        Args:
            trimestre: Trimestre a buscar arquivos
            
        Returns:
            Lista de caminhos dos arquivos encontrados
        """
        arquivos = []
        
        # Buscar recursivamente todos os arquivos do ano
        self._buscar_recursivo(
            f"{trimestre.ano}", 
            "", 
            arquivos
        )
        
        # Filtrar apenas arquivos do trimestre especificado
        arquivos_filtrados = []
        padrao_trimestre = re.compile(rf"{trimestre.numero}[tT]")
        
        for caminho in arquivos:
            if padrao_trimestre.search(caminho):
                arquivos_filtrados.append(caminho)
        
        return arquivos_filtrados
    
    def baixar_arquivo(self, arquivo: Arquivo, destino: str) -> bool:
        """Baixa um arquivo da API para o disco.
        
        Args:
            arquivo: Arquivo a baixar
            destino: Diretório de destino
            
        Returns:
            True se download bem-sucedido, False caso contrário
        """
        os.makedirs(destino, exist_ok=True)
        
        url = f"{self.url_base}/demonstracoes_contabeis/{arquivo.trimestre.ano}/{arquivo.caminho}"
        caminho_destino = os.path.join(destino, arquivo.nome_base)
        
        try:
            print(f"    Baixando {arquivo.nome_base}...", end=" ", flush=True)
            
            resposta = self.sessao.get(url, timeout=30, stream=True)
            resposta.raise_for_status()
            
            # Salvar arquivo em chunks
            with open(caminho_destino, 'wb') as f:
                for trecho in resposta.iter_content(chunk_size=8192):
                    f.write(trecho)
            
            # Verificar tamanho do arquivo
            tamanho_mb = os.path.getsize(caminho_destino) / (1024 * 1024)
            print(f"OK ({tamanho_mb:.1f} MB)")
            return True
        
        except requests.exceptions.RequestException as e:
            print(f"FALHA ({e})")
            return False
    
    def fechar(self):
        """Fecha a sessão HTTP."""
        self.sessao.close()
    
    def _buscar_recursivo(self, caminho_base: str, subpasta: str, arquivos: List[str]):
        """Busca arquivos recursivamente em uma estrutura de diretórios da API.
        
        Args:
            caminho_base: Caminho base (geralmente o ano)
            subpasta: Subpasta atual sendo navegada
            arquivos: Lista para acumular caminhos encontrados
        """
        caminho_completo = f"{caminho_base}/{subpasta}" if subpasta else caminho_base
        url = f"{self.url_base}/demonstracoes_contabeis/{caminho_completo}/"
        
        try:
            resposta = self.sessao.get(url, timeout=10)
            resposta.raise_for_status()
            
            # Extrair todos os links
            padrao_link = r'href="([^"]+)"'
            links = re.findall(padrao_link, resposta.text)
            
            for link in links:
                # Ignorar links especiais
                if link in ['..', '.', 'Parent Directory']:
                    continue
                
                # Se termina com /, é uma pasta - navegar recursivamente
                if link.endswith('/'):
                    pasta_limpa = link.rstrip('/')
                    nova_subpasta = f"{subpasta}/{pasta_limpa}" if subpasta else pasta_limpa
                    self._buscar_recursivo(caminho_base, nova_subpasta, arquivos)
                else:
                    # É um arquivo - adicionar à lista
                    caminho_arquivo = f"{subpasta}/{link}" if subpasta else link
                    arquivos.append(caminho_arquivo)
        
        except requests.exceptions.RequestException:
            pass  # Silenciosamente ignorar erros de navegação
