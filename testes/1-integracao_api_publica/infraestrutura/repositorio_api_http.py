import requests
import re
from typing import List
from domain.entidades import Trimestre, Arquivo
from domain.repositorios import RepositorioAPI

class RepositorioAPIHTTP(RepositorioAPI):
    def __init__(self, url_base: str):
        self.url_base = url_base
        self.sessao = requests.Session()
    
    def obter_anos_disponiveis(self) -> List[int]:
        url = f"{self.url_base}/demonstracoes_contabeis/"
        try:
            resposta = self.sessao.get(url, timeout=10)
            resposta.raise_for_status()
            
            padrao = r'href="([^"]+/)"'
            matches = re.findall(padrao, resposta.text)
            
            anos = []
            for match in matches:
                pasta = match.rstrip('/')
                if pasta.isdigit():
                    anos.append(int(pasta))
            
            return anos
        except requests.exceptions.RequestException:
            return []
    
    def obter_trimestres_do_ano(self, ano: int) -> List[str]:
        try:
            print(f"Procurando últimos trimestres disponíveis para {ano}...", flush=True)
            arquivos = []
            self._buscar_recursivo(f"{ano}", "", arquivos)
            
            trimestres_encontrados = set()
            for caminho in arquivos:
                match = re.search(r'(\d)[tT]', caminho)
                if match:
                    trimestres_encontrados.add(f"{match.group(1)}T")
            
            trimestres_validos = sorted(trimestres_encontrados)
            print(f"Trimestres encontrados para {ano}: {trimestres_validos}", flush=True)
            return trimestres_validos
        except requests.exceptions.RequestException:
            return []
    
    def obter_arquivos_do_trimestre(self, trimestre: Trimestre) -> List[str]:
        arquivos = []
        self._buscar_recursivo(
            f"{trimestre.ano}", 
            "", 
            arquivos
        )
        
        arquivos_filtrados = []
        padrao_trimestre = re.compile(rf"{trimestre.numero}[tT]")
        for caminho in arquivos:
            if padrao_trimestre.search(caminho):
                arquivos_filtrados.append(caminho)
        
        return arquivos_filtrados
    
    def _buscar_recursivo(self, caminho_base: str, subpasta: str, arquivos: List[str]):
        caminho_completo = f"{caminho_base}/{subpasta}" if subpasta else caminho_base
        url = f"{self.url_base}/demonstracoes_contabeis/{caminho_completo}/"
        
        try:
            resposta = self.sessao.get(url, timeout=10)
            resposta.raise_for_status()
            
            padrao_link = r'href="([^"]+)"'
            padrao_pasta = r'href="([^"]+/)"'
            
            links = re.findall(padrao_link, resposta.text)
            for link in links:
                if link.endswith('/'):
                    continue
                if link in ['..', '.', 'Parent Directory']:
                    continue
                caminho_arquivo = f"{subpasta}/{link}" if subpasta else link
                arquivos.append(caminho_arquivo)
            
            pastas = re.findall(padrao_pasta, resposta.text)
            for pasta in pastas:
                pasta_limpa = pasta.rstrip('/')
                if pasta_limpa not in ['..', '.', 'Parent Directory']:
                    nova_subpasta = f"{subpasta}/{pasta_limpa}" if subpasta else pasta_limpa
                    self._buscar_recursivo(caminho_base, nova_subpasta, arquivos)
        except requests.exceptions.RequestException:
            pass
    
    def baixar_arquivo(self, arquivo: Arquivo, destino: str) -> bool:
        import os
        
        os.makedirs(destino, exist_ok=True)
        
        url = f"{self.url_base}/demonstracoes_contabeis/{arquivo.trimestre.ano}/{arquivo.caminho}"
        caminho_destino = os.path.join(destino, arquivo.nome_base)
        
        try:
            print(f"  Fazendo download do arquivo {arquivo.nome_base}...", flush=True)
            resposta = self.sessao.get(url, timeout=30, stream=True)
            resposta.raise_for_status()
            
            with open(caminho_destino, 'wb') as f:
                for trecho in resposta.iter_content(chunk_size=8192):
                    f.write(trecho)
            
            return True
        except requests.exceptions.RequestException:
            return False
    
    def fechar(self):
        self.sessao.close()
