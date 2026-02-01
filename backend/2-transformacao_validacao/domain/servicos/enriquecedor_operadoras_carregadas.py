"""
Serviço responsável pelo enriquecimento de dados de operadoras após carregamento.
Adiciona dados derivados (CNPJ limpo, status em uppercase, etc)
"""
import pandas as pd

from .validador_cnpj import ValidadorCNPJ


class EnriquecedorOperadorasCarregadas:
    """Enriquece dados brutos de operadoras com informações derivadas"""

    @staticmethod
    def enriquecer(df: pd.DataFrame) -> pd.DataFrame:
        """
        Enriquece DataFrame bruto de operadoras com dados derivados.
        
        Args:
            df: DataFrame bruto com operadoras
        
        Returns:
            DataFrame enriquecido com colunas adicionais
        """
        if df.empty:
            return df

        df = df.copy()

        # Normalizar colunas para lowercase
        df.columns = df.columns.str.lower().str.strip()

        # Adicionar CNPJ limpo
        df["cnpj_limpo"] = df["cnpj"].astype(str).apply(ValidadorCNPJ.limpar)

        # Adicionar status em uppercase para comparações
        df["status_upper"] = df["status"].astype(str).str.upper().str.strip()

        return df
