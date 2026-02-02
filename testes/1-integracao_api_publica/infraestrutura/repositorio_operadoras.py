"""Repositório de Operadoras - Busca e carrega tabelas de operadoras da ANS.

Realiza download das tabelas de operadoras ativas e canceladas,
extrai e armazena em arquivos CSV para enriquecimento de dados.
"""

import os
import pandas as pd
import requests
from typing import Dict, Tuple
from io import BytesIO

from config import DIRETORIO_OPERADORAS
from infraestrutura.logger import get_logger

logger = get_logger('RepositorioOperadoras')


class RepositorioOperadoras:
    """Repositório especializado em gerenciar dados de operadoras da ANS."""
    
    BASE_URL_ATIVAS = "https://dadosabertos.ans.gov.br/FTP/PDA/operadoras_de_plano_de_saude_ativas/"
    BASE_URL_CANCELADAS = "https://dadosabertos.ans.gov.br/FTP/PDA/operadoras_de_plano_de_saude_canceladas/"
    ARQUIVOS_ATIVAS = ["Relatorio_cadop.csv"]
    ARQUIVOS_CANCELADAS = ["Relatorio_cadop_canceladas.csv"]
    
    TIMEOUT = 30
    
    def __init__(self):
        self.diretorio = DIRETORIO_OPERADORAS
        
        # Garantir que o diretório existe e está acessível
        if os.path.exists(self.diretorio):
            # Tentar limpar arquivos antigos
            for arquivo in ["operadoras_ativas.csv", "operadoras_canceladas.csv"]:
                caminho = os.path.join(self.diretorio, arquivo)
                if os.path.exists(caminho):
                    try:
                        os.remove(caminho)
                        logger.debug(f"Arquivo antigo removido: {arquivo}")
                    except:
                        # Se não conseguir remover, usar timestamp
                        import time
                        timestamp = int(time.time())
                        try:
                            os.rename(caminho, f"{caminho}.{timestamp}")
                            logger.debug(f"Arquivo antigo renomeado: {arquivo}")
                        except:
                            pass
        
        os.makedirs(self.diretorio, exist_ok=True)
        
        self.arquivo_ativas = os.path.join(self.diretorio, "operadoras_ativas.csv")
        self.arquivo_canceladas = os.path.join(self.diretorio, "operadoras_canceladas.csv")
    
    def carregar(self) -> Dict:
        """Carrega as tabelas de operadoras ativas e canceladas.
        
        Returns:
            Dict com resultado do carregamento
        """
        resultado = {
            'ativas': False,
            'canceladas': False,
            'total_ativas': 0,
            'total_canceladas': 0,
            'erros': []
        }
        
        logger.info("Iniciando carregamento de tabelas de operadoras da ANS")
        
        try:
            resultado['ativas'], resultado['total_ativas'] = self._carregar_ativas()
        except Exception as e:
            erro_msg = f"Erro ao carregar operadoras ativas: {str(e)}"
            logger.error(erro_msg)
            resultado['erros'].append(erro_msg)
            print(f"  ❌ {erro_msg}")
        
        try:
            resultado['canceladas'], resultado['total_canceladas'] = self._carregar_canceladas()
        except Exception as e:
            erro_msg = f"Erro ao carregar operadoras canceladas: {str(e)}"
            logger.error(erro_msg)
            resultado['erros'].append(erro_msg)
            print(f"  ❌ {erro_msg}")
        
        logger.info(
            f"Carregamento concluído - Ativas: {resultado['total_ativas']}, "
            f"Canceladas: {resultado['total_canceladas']}"
        )
        
        return resultado
    
    def _carregar_ativas(self) -> Tuple[bool, int]:
        """Carrega tabela de operadoras ativas.
        
        Returns:
            (sucesso: bool, total_registros: int)
        """
        print("\n  📥 Buscando operadoras ativas...")
        
        # Deletar arquivo existente se houver
        if os.path.exists(self.arquivo_ativas):
            try:
                os.remove(self.arquivo_ativas)
            except Exception as e:
                logger.warning(f"Não foi possível deletar arquivo existente: {e}")
        
        # Tentar nomes conhecidos
        for nome_arquivo in self.ARQUIVOS_ATIVAS:
            url = f"{self.BASE_URL_ATIVAS}{nome_arquivo}"
            try:
                df = self._baixar_arquivo(url, 'csv')
                if df is not None and not df.empty:
                    df = self._normalizar_colunas_operadoras(df)
                    df.to_csv(self.arquivo_ativas, index=False, encoding='utf-8', sep=';')
                    logger.info(f"Operadoras ativas carregadas: {len(df)} registros")
                    print(f"     [OK] {len(df)} operadoras ativas carregadas")
                    return True, len(df)
            except Exception as e:
                logger.debug(f"Falha ao baixar {nome_arquivo}: {str(e)}")
                continue
        
        raise Exception("Não foi possível baixar operadoras ativas em nenhum formato")
    
    def _carregar_canceladas(self) -> Tuple[bool, int]:
        """Carrega tabela de operadoras canceladas.
        
        Returns:
            (sucesso: bool, total_registros: int)
        """
        print("  📥 Buscando operadoras canceladas...")
        
        # Deletar arquivo existente se houver
        if os.path.exists(self.arquivo_canceladas):
            try:
                os.remove(self.arquivo_canceladas)
            except Exception as e:
                logger.warning(f"Não foi possível deletar arquivo existente: {e}")
        
        # Tentar nomes conhecidos
        for nome_arquivo in self.ARQUIVOS_CANCELADAS:
            url = f"{self.BASE_URL_CANCELADAS}{nome_arquivo}"
            try:
                df = self._baixar_arquivo(url, 'csv')
                if df is not None and not df.empty:
                    df = self._normalizar_colunas_operadoras(df)
                    df.to_csv(self.arquivo_canceladas, index=False, encoding='utf-8', sep=';')
                    logger.info(f"Operadoras canceladas carregadas: {len(df)} registros")
                    print(f"     [OK] {len(df)} operadoras canceladas carregadas")
                    return True, len(df)
            except Exception as e:
                logger.debug(f"Falha ao baixar {nome_arquivo}: {str(e)}")
                continue
        
        raise Exception("Não foi possível baixar operadoras canceladas em nenhum formato")
    
    def _baixar_arquivo(self, url: str, extensao: str) -> pd.DataFrame:
        """Baixa e carrega arquivo da URL.
        
        Args:
            url: URL do arquivo
            extensao: Extensão do arquivo (xlsx, csv, xls)
        
        Returns:
            DataFrame com os dados ou None se falhar
        """
        response = requests.get(url, timeout=self.TIMEOUT)
        response.raise_for_status()
        
        if extensao == 'csv':
            # Tentar com múltiplos encodings para garantir leitura correta
            encodings = ['utf-8', 'latin-1', 'iso-8859-1', 'cp1252']
            for encoding in encodings:
                try:
                    return pd.read_csv(
                        BytesIO(response.content),
                        sep=';',
                        encoding=encoding,
                        on_bad_lines='skip'
                    )
                except:
                    continue
            
            # Se nenhum encoding funcionou, tentar com response.text
            return pd.read_csv(
                pd.io.common.StringIO(response.text),
                sep=';',
                encoding='utf-8',
                on_bad_lines='skip'
            )
        elif extensao in ['xlsx', 'xls']:
            return pd.read_excel(BytesIO(response.content))
        
        return None
    
    def _normalizar_colunas_operadoras(self, df: pd.DataFrame) -> pd.DataFrame:
        """Normaliza os nomes e conteúdo das colunas da tabela de operadoras.
        
        Args:
            df: DataFrame com dados brutos
        
        Returns:
            DataFrame normalizado
        """
        # Normalizar nomes de colunas
        df.columns = df.columns.str.upper().str.strip().str.replace(' ', '_')
        
        # Tentar mapear REGISTRO/REGISTRO_OPERADORA ou REG_ANS para REG_ANS
        if 'REGISTRO_OPERADORA' in df.columns:
            df.rename(columns={'REGISTRO_OPERADORA': 'REG_ANS'}, inplace=True)
        elif 'REGISTRO' in df.columns:
            df.rename(columns={'REGISTRO': 'REG_ANS'}, inplace=True)
        elif 'REG_ANS' not in df.columns:
            # Tentar alternativas
            mapa_colunas = {
                'RAZSOC': 'RAZAO_SOCIAL',
                'MODALIDADE_OPERADORA': 'MODALIDADE'
            }
            
            for df_col in df.columns:
                for chave, valor in mapa_colunas.items():
                    if chave.lower() == df_col.lower():
                        df.rename(columns={df_col: valor}, inplace=True)
        
        # Colunas necessárias
        colunas_necessarias = ['REG_ANS', 'CNPJ', 'RAZAO_SOCIAL', 'MODALIDADE', 'UF']
        
        # Garantir que temos as colunas necessárias
        for col in colunas_necessarias:
            if col not in df.columns:
                df[col] = ''
        
        # Selecionar e ordenar colunas
        df = df[colunas_necessarias].copy()
        
        # Converter para uppercase para melhor comparação
        df['REG_ANS'] = df['REG_ANS'].astype(str).str.strip()
        df['CNPJ'] = df['CNPJ'].astype(str).str.strip()
        df['RAZAO_SOCIAL'] = df['RAZAO_SOCIAL'].astype(str).str.strip()
        df['MODALIDADE'] = df['MODALIDADE'].astype(str).str.strip().str.upper()
        df['UF'] = df['UF'].astype(str).str.strip().str.upper()
        
        return df
    
    def obter_operadora(self, registro: str) -> Dict:
        """Busca dados da operadora pelo registro em ambas as tabelas.
        
        Args:
            registro: Número do registro da operadora
        
        Returns:
            Dict com dados da operadora ou indicação de N/L ou CONFLITO
        """
        registro = str(registro).strip()
        
        resultado = {
            'registro': registro,
            'cnpj': 'N/L',
            'razao_social': 'N/L',
            'modalidade': 'N/L',
            'uf': 'N/L',
            'status': 'NAO_LOCALIZADO'
        }
        
        matches = []
        
        # Buscar em operadoras ativas
        if os.path.exists(self.arquivo_ativas):
            try:
                df_ativas = pd.read_csv(self.arquivo_ativas, sep=';', encoding='utf-8')
                match = df_ativas[df_ativas['REG_ANS'].astype(str).str.strip() == registro]
                if not match.empty:
                    for _, row in match.iterrows():
                        matches.append({
                            'cnpj': row.get('CNPJ', ''),
                            'razao_social': row.get('RAZAO_SOCIAL', ''),
                            'modalidade': row.get('MODALIDADE', ''),
                            'uf': row.get('UF', ''),
                            'tipo': 'ATIVA'
                        })
            except Exception as e:
                logger.error(f"Erro ao buscar em operadoras ativas: {str(e)}")
        
        # Buscar em operadoras canceladas
        if os.path.exists(self.arquivo_canceladas):
            try:
                df_canceladas = pd.read_csv(self.arquivo_canceladas, sep=';', encoding='utf-8')
                match = df_canceladas[df_canceladas['REG_ANS'].astype(str).str.strip() == registro]
                if not match.empty:
                    for _, row in match.iterrows():
                        matches.append({
                            'cnpj': row.get('CNPJ', ''),
                            'razao_social': row.get('RAZAO_SOCIAL', ''),
                            'modalidade': row.get('MODALIDADE', ''),
                            'uf': row.get('UF', ''),
                            'tipo': 'CANCELADA'
                        })
            except Exception as e:
                logger.error(f"Erro ao buscar em operadoras canceladas: {str(e)}")
        
        # Processar resultados
        if len(matches) == 0:
            resultado['status'] = 'NAO_LOCALIZADO'
            logger.warning(f"Operadora não localizada - Registro: {registro}")
        
        elif len(matches) == 1:
            match = matches[0]
            resultado['cnpj'] = match['cnpj']
            resultado['razao_social'] = match['razao_social']
            resultado['modalidade'] = match['modalidade']
            resultado['uf'] = match['uf']
            resultado['status'] = f"LOCALIZADO_{match['tipo']}"
        
        else:
            resultado['cnpj'] = 'CONFLITO'
            resultado['razao_social'] = 'CONFLITO'
            resultado['modalidade'] = 'CONFLITO'
            resultado['uf'] = 'CONFLITO'
            resultado['status'] = 'CONFLITO'
            logger.warning(
                f"Múltiplos registros encontrados para operadora - "
                f"Registro: {registro}, Total: {len(matches)}"
            )
        
        return resultado
