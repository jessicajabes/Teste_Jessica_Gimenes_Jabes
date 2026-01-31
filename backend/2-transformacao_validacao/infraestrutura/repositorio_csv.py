"""
Repositório de arquivo CSV para transformação e validação
"""

import pandas as pd
import os
from domain.repositorios import RepositorioCSV

class RepositorioCSVLocal(RepositorioCSV):
    """Implementação local para leitura de arquivos CSV"""
    
    def ler_arquivo(self, caminho: str) -> pd.DataFrame:
        """Lê um arquivo CSV"""
        try:
            if not os.path.exists(caminho):
                print(f"✗ Arquivo não encontrado: {caminho}")
                return pd.DataFrame()
            
            df = pd.read_csv(caminho, sep=';', encoding='utf-8')
            print(f"✓ {os.path.basename(caminho)}: {len(df)} registros carregados")
            return df
        except Exception as e:
            print(f"✗ Erro ao carregar {caminho}: {e}")
            return pd.DataFrame()
