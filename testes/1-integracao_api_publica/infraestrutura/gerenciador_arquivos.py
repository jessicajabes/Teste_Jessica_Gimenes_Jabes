"""Gerenciador de Arquivos - Extração de ZIPs e manipulação de arquivos."""

import os
import zipfile
from typing import List


class GerenciadorArquivos:
    """Gerencia operações com arquivos locais (extração, listagem, etc)."""
    
    def extrair_zips(self, diretorio: str) -> None:
        """Extrai todos os arquivos ZIP em um diretório.
        
        Args:
            diretorio: Diretório contendo os arquivos ZIP
        """
        if not os.path.exists(diretorio):
            print(f" Diretório não encontrado: {diretorio}")
            return
        
        arquivos_zip = [f for f in os.listdir(diretorio) if f.endswith('.zip')]
        
        if not arquivos_zip:
            print(" Nenhum arquivo ZIP encontrado")
            return
        
        print(f"  Extraindo {len(arquivos_zip)} arquivos ZIP...")
        
        for arquivo_zip in arquivos_zip:
            caminho_zip = os.path.join(diretorio, arquivo_zip)
            diretorio_extracao = os.path.join(diretorio, 'extracted')
            
            try:
                with zipfile.ZipFile(caminho_zip, 'r') as zip_ref:
                    zip_ref.extractall(diretorio_extracao)
                print(f"    ✓ {arquivo_zip}")
            except Exception as e:
                print(f"    ✗ Erro ao extrair {arquivo_zip}: {e}")
    
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
