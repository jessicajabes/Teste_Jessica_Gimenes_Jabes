"""Serviço de Domínio para Processamento de Arquivos.

Contém toda a lógica de negócio relacionada a leitura, 
extração e transformação de arquivos de dados.
"""

import os
import pandas as pd
from typing import List, Dict, Optional, Tuple

from infraestrutura.logger import get_logger
from .validador_normalizador import ValidadorNormalizador

logger = get_logger('ProcessadorArquivos')


class ProcessadorArquivos:
    """Lógica de negócio para processar arquivos de dados."""
    
    PALAVRAS_CHAVE = ["Despesas com Eventos/Sinistros"]
    ENCODINGS = ['utf-8', 'latin-1', 'iso-8859-1', 'cp1252']
    
    @staticmethod
    def ler_arquivo_com_encoding(
        caminho: str,
        sep: str = ';',
        **kwargs
    ) -> Optional[pd.DataFrame]:
        """Tenta ler arquivo com múltiplos encodings."""
        for encoding in ProcessadorArquivos.ENCODINGS:
            try:
                df = pd.read_csv(
                    caminho,
                    sep=sep,
                    encoding=encoding,
                    quotechar='"',
                    on_bad_lines='skip',
                    **kwargs
                )
                logger.debug(
                    f"Arquivo {os.path.basename(caminho)} lido com sucesso usando encoding: {encoding}"
                )
                return df
            except Exception as e:
                logger.debug(f"Falha ao ler com {encoding}: {str(e)[:50]}")
                continue
        
        logger.error(f"Não foi possível ler arquivo com nenhum encoding: {caminho}")
        return None
    
    @staticmethod
    def contem_palavras_chave(caminho_arquivo: str = None, df: Optional[pd.DataFrame] = None) -> bool:
        """Verifica se o arquivo contém as palavras-chave esperadas."""
        try:
            if df is None:
                if caminho_arquivo is None:
                    return False
                if caminho_arquivo.endswith('.csv'):
                    df = ProcessadorArquivos.ler_arquivo_com_encoding(caminho_arquivo, sep=';')
                elif caminho_arquivo.endswith('.txt'):
                    df = ProcessadorArquivos.ler_arquivo_com_encoding(caminho_arquivo, sep='\t')
                elif caminho_arquivo.endswith('.xlsx'):
                    df = pd.read_excel(caminho_arquivo)
                else:
                    return False
            
            if df is None or df.empty:
                return False
            
            amostra = df.head(5000) if len(df) > 5000 else df
            colunas_texto = [c for c in amostra.columns if 'DESCR' in c or 'DESCRICAO' in c]
            
            alvo = amostra[colunas_texto] if colunas_texto else amostra.iloc[:, : min(10, amostra.shape[1])]
            
            for palavra in ProcessadorArquivos.PALAVRAS_CHAVE:
                if alvo.astype(str).apply(
                    lambda col: col.str.contains(palavra, case=False, na=False)
                ).any().any():
                    return True
            
            return False
        except Exception as e:
            logger.error(f"Erro ao verificar palavras-chave no arquivo: {e}")
            return False
    
    @staticmethod
    def extrair_dados_arquivo(
        caminho_arquivo: str,
        ano: int,
        trimestre: int
    ) -> Tuple[List[Dict], float, int]:
        """Extrai dados de um arquivo.
        
        Retorna:
            (dados, valor_arquivo, registros_rejeitados)
        """
        try:
            if caminho_arquivo.endswith('.csv'):
                df = ProcessadorArquivos.ler_arquivo_com_encoding(caminho_arquivo, sep=';')
            elif caminho_arquivo.endswith('.txt'):
                df = ProcessadorArquivos.ler_arquivo_com_encoding(caminho_arquivo, sep='\t')
            elif caminho_arquivo.endswith('.xlsx'):
                df = pd.read_excel(caminho_arquivo)
            else:
                return [], 0.0, 0
            
            if df is None:
                return [], 0.0, 0
            
            print(f"    Linhas no arquivo: {len(df)}")
            print(f"    Colunas: {list(df.columns)[:5]}...")
            
            # Verificar se o arquivo contém a palavra-chave (sem reler o arquivo)
            if not ProcessadorArquivos.contem_palavras_chave(df=df):
                print(f"    Arquivo não contém '{ProcessadorArquivos.PALAVRAS_CHAVE[0]}', pulando...")
                return [], 0.0, 0
            
            print(f"    Arquivo contém '{ProcessadorArquivos.PALAVRAS_CHAVE[0]}', processando todos os dados...")
            
            df.columns = df.columns.str.upper().str.strip().str.replace(' ', '_')
            
            dados = []
            registros_rejeitados = 0
            
            # Usar itertuples() para melhor performance
            colunas = list(df.columns)
            for idx, linha in enumerate(df.itertuples(index=False, name=None)):
                registro = dict(zip(colunas, linha))
                
                # Validar dados mínimos
                if ValidadorNormalizador.validar_registro(registro):
                    registro['ANO'] = ano
                    registro['TRIMESTRE'] = trimestre
                    dados.append(registro)
                else:
                    registros_rejeitados += 1
            
            valor_arquivo = ValidadorNormalizador.calcular_valor_arquivo(dados)
            
            return dados, valor_arquivo, registros_rejeitados
            
        except Exception as e:
            logger.error(f"Erro ao extrair dados do arquivo {caminho_arquivo}: {e}")
            return [], 0.0, 0
