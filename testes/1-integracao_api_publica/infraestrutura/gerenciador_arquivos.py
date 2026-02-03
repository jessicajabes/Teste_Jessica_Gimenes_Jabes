"""Gerenciador de Arquivos - Extração de ZIPs e manipulação de arquivos."""

import os
import zipfile
import shutil
from typing import List


class GerenciadorArquivos:
    """Gerencia operações com arquivos locais (extração, listagem, etc)."""
    
    def extrair_zips(self, diretorio: str) -> None:
        """Extrai todos os arquivos ZIP em um diretório.
        
        Tratamento especial:
        - Relatorio_cadop*.csv: copia para /operadoras
        - Outros ZIPs: extrai para /extracted
        
        Args:
            diretorio: Diretório contendo os arquivos ZIP
        """
        if not os.path.exists(diretorio):
            print(f" Diretório não encontrado: {diretorio}")
            return
        
        # Primeiro, copiar CSVs de operadoras
        self._copiar_csvs_operadoras(diretorio)
        
        # Depois, extrair ZIPs
        arquivos_zip = [f for f in os.listdir(diretorio) if f.endswith('.zip')]
        
        if not arquivos_zip:
            if not os.path.exists(os.path.join(diretorio, 'operadoras')):
                print(" Nenhum arquivo ZIP encontrado")
            return
        
        print(f"  Extraindo {len(arquivos_zip)} arquivos ZIP...")
        
        for arquivo_zip in arquivos_zip:
            caminho_zip = os.path.join(diretorio, arquivo_zip)
            diretorio_extracao = os.path.join(diretorio, 'extracted')
            
            try:
                with zipfile.ZipFile(caminho_zip, 'r') as zip_ref:
                    zip_ref.extractall(diretorio_extracao)
                print(f"    [OK] {arquivo_zip}")
            except Exception as e:
                print(f"    [ERRO] Erro ao extrair {arquivo_zip}: {e}")
    
    def _copiar_csvs_operadoras(self, diretorio: str) -> None:
        """Copia CSVs de operadoras para a pasta /operadoras.
        
        Args:
            diretorio: Diretório contendo os CSVs
        """
        diretorio_operadoras = os.path.join(diretorio, 'operadoras')
        os.makedirs(diretorio_operadoras, exist_ok=True)
        
        csvs_copiados = False
        
        # Procurar por CSVs de operadoras
        arquivos_no_dir = os.listdir(diretorio)
        for arquivo in arquivos_no_dir:
            if arquivo.startswith('Relatorio_cadop') and arquivo.endswith('.csv'):
                origem = os.path.join(diretorio, arquivo)
                destino = os.path.join(diretorio_operadoras, arquivo)
                
                try:
                    shutil.copy2(origem, destino)
                    print(f"    [COPY] {arquivo} -> operadoras/")
                    csvs_copiados = True
                except Exception as e:
                    print(f"    [ERRO] Erro ao copiar {arquivo}: {e}")
    
    def listar_csvs(self, diretorio: str) -> List[str]:
        """Lista todos os arquivos CSV em um diretório (recursivo).
        
        Args:
            diretorio: Diretório raiz para buscar CSVs
            
        Returns:
            Lista de caminhos completos dos arquivos CSV
        """
        csvs = []
        
        for raiz, _, arquivos in os.walk(diretorio):
            for arquivo in arquivos:
                if arquivo.endswith('.csv'):
                    caminho_completo = os.path.join(raiz, arquivo)
                    csvs.append(caminho_completo)
        
        return csvs
