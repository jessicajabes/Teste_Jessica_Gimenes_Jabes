import os
import zipfile
import pandas as pd
from typing import List
from domain.repositorios import RepositorioArquivo
from config import DIRETORIO_ZIPS, DIRETORIO_EXTRAIDO

class RepositorioArquivoLocal(RepositorioArquivo):
    ENCODINGS = ['utf-8', 'latin-1', 'iso-8859-1', 'cp1252']  # Ordem de prioridade
    
    @staticmethod
    def _ler_com_encoding(caminho: str, sep: str = ';', **kwargs) -> pd.DataFrame:
        """Tenta ler arquivo com múltiplos encodings"""
        for encoding in RepositorioArquivoLocal.ENCODINGS:
            try:
                df = pd.read_csv(caminho, sep=sep, encoding=encoding, quotechar='"', on_bad_lines='skip', **kwargs)
                return df
            except:
                continue
        
        # Se nenhum encoding funcionou, tenta o padrão
        return pd.read_csv(caminho, sep=sep, encoding='utf-8', quotechar='"', on_bad_lines='skip', **kwargs)
    def extrair_zips(self, diretorio: str) -> List[str]:
        base_dir = diretorio or os.path.dirname(DIRETORIO_ZIPS)
        diretorio_zips = os.path.join(base_dir, "zips")
        diretorio_extraido = os.path.join(base_dir, "extraido")
        os.makedirs(diretorio_zips, exist_ok=True)
        os.makedirs(diretorio_extraido, exist_ok=True)
        
        arquivos_extraidos = []
        
        arquivos_zip = [f for f in os.listdir(diretorio_zips) if f.endswith('.zip')]
        print(f"\nArquivos ZIP encontrados: {len(arquivos_zip)}")
        
        for nome_arquivo in arquivos_zip:
            caminho_zip = os.path.join(diretorio_zips, nome_arquivo)
            
            try:
                print(f"  Extraindo {nome_arquivo}...")
                with zipfile.ZipFile(caminho_zip, 'r') as zip_ref:
                    zip_ref.extractall(diretorio_extraido)
                    print(f"    Extraído: {len(zip_ref.namelist())} arquivo(s)")
                arquivos_extraidos.append(nome_arquivo)
            except Exception as e:
                print(f"    Erro ao extrair: {e}")
        
        return arquivos_extraidos
    
    def encontrar_arquivos_dados(self, diretorio: str) -> dict:
        base_dir = diretorio or os.path.dirname(DIRETORIO_EXTRAIDO)
        diretorio_extraido = os.path.join(base_dir, "extraido")
        arquivos_encontrados = {'csv': [], 'txt': [], 'xlsx': []}
        
        if not os.path.exists(diretorio_extraido):
            print(f"\nDiretório extraído não existe: {diretorio_extraido}")
            return arquivos_encontrados
        
        for raiz, _, arquivos in os.walk(diretorio_extraido):
            for arquivo in arquivos:
                caminho_arquivo = os.path.join(raiz, arquivo)
                
                if arquivo.endswith('.csv'):
                    arquivos_encontrados['csv'].append(caminho_arquivo)
                elif arquivo.endswith('.txt'):
                    arquivos_encontrados['txt'].append(caminho_arquivo)
                elif arquivo.endswith('.xlsx'):
                    arquivos_encontrados['xlsx'].append(caminho_arquivo)
        
        total = sum(len(v) for v in arquivos_encontrados.values())
        print(f"\nArquivos de dados encontrados: {total}")
        print(f"  CSV: {len(arquivos_encontrados['csv'])}")
        print(f"  TXT: {len(arquivos_encontrados['txt'])}")
        print(f"  XLSX: {len(arquivos_encontrados['xlsx'])}")
        
        return arquivos_encontrados
    
    def ler_arquivo(self, caminho: str) -> pd.DataFrame:
        try:
            if caminho.endswith('.csv'):
                df = self._ler_com_encoding(caminho, sep=';')
                print(f"\n[DEBUG] Leitura do arquivo {caminho}")
                print(f"[DEBUG] Shape: {df.shape}")
                print(f"[DEBUG] Colunas reais: {list(df.columns)}")
                print(f"[DEBUG] Primeira linha:\n{df.iloc[0] if len(df) > 0 else 'Vazio'}")
                return df
            elif caminho.endswith('.txt'):
                return self._ler_com_encoding(caminho, sep='\t')
            elif caminho.endswith('.xlsx'):
                return pd.read_excel(caminho)
        except Exception as e:
            print(f"[DEBUG] Erro ao ler arquivo: {e}")
            return pd.DataFrame()
    
    def salvar_csv(self, dados: pd.DataFrame, caminho: str) -> bool:
        try:
            dados.to_csv(caminho, index=False, encoding='utf-8-sig')
            return True
        except Exception:
            return False
