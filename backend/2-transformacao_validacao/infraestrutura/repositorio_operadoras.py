"""
Repositório de operadoras - Carrega dados do banco de dados
Responsável apenas pela infraestrutura de acesso ao banco
"""
import logging
from typing import Optional
import pandas as pd
from sqlalchemy import create_engine


class RepositorioOperadoras:
    """Repositório especializado em operadoras do banco de dados"""

    def __init__(self, database_url: str):
        self.database_url = database_url
        self._normalize_url()

    def _normalize_url(self):
        """Normaliza URL do PostgreSQL para psycopg2"""
        if self.database_url.startswith("postgresql://"):
            self.database_url = self.database_url.replace("postgresql://", "postgresql+psycopg2://", 1)

    def carregar(self, logger: logging.Logger) -> pd.DataFrame:
        """
        Carrega operadoras do banco de dados (apenas coletas brutos).
        
        Args:
            logger: Logger para registrar erros
        
        Returns:
            DataFrame bruto com operadoras ou vazio se houver erro
        """
        try:
            engine = create_engine(self.database_url, echo=False)
            df = pd.read_sql_query(
                "SELECT cnpj, reg_ans, modalidade, uf, status FROM operadoras",
                engine,
            )
            engine.dispose()

            if df.empty:
                logger.warning("Tabela de operadoras vazia")

            return df

        except Exception as e:
            logger.error(f"Erro ao carregar operadoras do banco: {e}")
            return pd.DataFrame(
                columns=["cnpj", "reg_ans", "modalidade", "uf", "status"]
            )
