"""
Serviço responsável pelo carregamento de dados de diferentes fontes.
Orquestra repositórios e serviços de domínio para carregar e enriquecer dados.
"""
import os
import logging
from typing import Optional
import pandas as pd

from .gerenciador_zip import GerenciadorZIP
from .enriquecedor_operadoras_carregadas import EnriquecedorOperadorasCarregadas


class CarregadorDados:
    """Gerencia o carregamento de dados de CSV e banco de dados"""

    @staticmethod
    def carregar_despesas(
        nome_arquivo,
        zip_path: Optional[str],
        diretorio_dados: str,
        logger: logging.Logger,
    ) -> Optional[pd.DataFrame]:
        """
        Carrega arquivo CSV de despesas, tentando do ZIP primeiro e depois do sistema de arquivos.
        
        Args:
            nome_arquivo: Nome do arquivo CSV ou lista de nomes
            zip_path: Caminho do ZIP (pode ser None)
            diretorio_dados: Diretório onde procurar arquivos CSV
            logger: Logger para registrar erros
        
        Returns:
            DataFrame com os dados ou None se não encontrado
        """
        # Se receber uma lista, tentar cada arquivo
        if isinstance(nome_arquivo, list):
            for nome in nome_arquivo:
                df = CarregadorDados.carregar_despesas(nome, zip_path, diretorio_dados, logger)
                if df is not None:
                    return df
            return None

        # Tentar carregar do ZIP
        if zip_path:
            df = GerenciadorZIP.ler_csv_do_zip(zip_path, nome_arquivo)
            if df is not None:
                print(f"✓ Carregado do ZIP: {nome_arquivo} ({len(df)} registros)")
                return df

        # Tentar carregar do sistema de arquivos
        caminho = os.path.join(diretorio_dados, nome_arquivo)
        if os.path.exists(caminho):
            try:
                df = pd.read_csv(caminho, sep=";", encoding="utf-8-sig")
                print(f"✓ Carregado: {nome_arquivo} ({len(df)} registros)")
                return df
            except Exception as e:
                logger.error(f"Erro ao ler {nome_arquivo}: {e}")
                return None

        logger.error(f"Arquivo não encontrado: {nome_arquivo}")
        return None

    @staticmethod
    def carregar_operadoras(database_url: str, logger: logging.Logger) -> pd.DataFrame:
        """
        Carrega e enriquece operadoras do banco de dados.
        
        Args:
            database_url: URL de conexão com o banco
            logger: Logger para registrar erros
        
        Returns:
            DataFrame com operadoras enriquecidas (vazio se houver erro)
        """
        from infraestrutura.repositorio_operadoras import RepositorioOperadoras

        # Carregar dados brutos do repositório (infraestrutura)
        repo = RepositorioOperadoras(database_url)
        df = repo.carregar(logger)

        # Enriquecer dados com lógica de domínio
        if not df.empty:
            df = EnriquecedorOperadorasCarregadas.enriquecer(df)

        return df
    
    @staticmethod
    def carregar_operadoras_de_csvs(diretorio_downloads: str, logger: logging.Logger) -> pd.DataFrame:
        """
        Carrega operadoras ATIVAS e CANCELADAS dos CSVs gerados pelo Teste 1.
        
        Args:
            diretorio_downloads: Diretório raiz dos downloads
            logger: Logger para registrar erros
        
        Returns:
            DataFrame com TODAS as operadoras (ativas + canceladas)
        """
        import pandas as pd
        
        # Buscar arquivos de operadoras
        pasta_operadoras = os.path.join(diretorio_downloads, "operadoras")
        ativas_path = os.path.join(pasta_operadoras, "operadoras_ativas.csv")
        canceladas_path = os.path.join(pasta_operadoras, "operadoras_canceladas.csv")
        
        dfs = []
        
        # Carregar ativas
        if os.path.exists(ativas_path):
            try:
                ativas = pd.read_csv(ativas_path, sep=';', encoding='utf-8-sig')
                ativas.columns = ativas.columns.str.lower().str.strip()
                ativas['status'] = 'ATIVA'
                dfs.append(ativas)
                logger.info(f"✓ {len(ativas)} operadoras ativas carregadas")
            except Exception as e:
                logger.error(f"Erro ao carregar operadoras ativas: {e}")
        
        # Carregar canceladas
        if os.path.exists(canceladas_path):
            try:
                canceladas = pd.read_csv(canceladas_path, sep=';', encoding='utf-8-sig')
                canceladas.columns = canceladas.columns.str.lower().str.strip()
                canceladas['status'] = 'CANCELADA'
                dfs.append(canceladas)
                logger.info(f"✓ {len(canceladas)} operadoras canceladas carregadas")
            except Exception as e:
                logger.error(f"Erro ao carregar operadoras canceladas: {e}")
        
        if not dfs:
            logger.error("Nenhum arquivo de operadoras encontrado")
            return pd.DataFrame()
        
        # Concatenar todas
        operadoras = pd.concat(dfs, ignore_index=True)
        
        # Normalizar coluna de registro ANS
        coluna_reg = None
        for coluna_possivel in ['registro_operadora', 'reg_ans', 'registro_ans', 'registro ans']:
            if coluna_possivel in operadoras.columns:
                coluna_reg = coluna_possivel
                break
        
        if coluna_reg and coluna_reg != 'reg_ans':
            operadoras.rename(columns={coluna_reg: 'reg_ans'}, inplace=True)
        
        # Enriquecer com lógica de domínio
        if not operadoras.empty:
            operadoras = EnriquecedorOperadorasCarregadas.enriquecer(operadoras)
        
        logger.info(f"✓ Total: {len(operadoras)} operadoras carregadas (ativas + canceladas)")
        return operadoras
