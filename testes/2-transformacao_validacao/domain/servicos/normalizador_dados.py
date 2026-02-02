"""Serviço de Domínio para Normalização de Dados."""

import re
import pandas as pd
from typing import Optional


class NormalizadorDados:
    """Normaliza dados de despesas e valores."""
    
    @staticmethod
    def normalizar_colunas(df: pd.DataFrame) -> pd.DataFrame:
        """Normaliza nomes de colunas para padrão uniforme.
        
        Args:
            df: DataFrame a normalizar
            
        Returns:
            DataFrame com colunas normalizadas (inplace)
        """
        mapping = {}
        
        for col in df.columns:
            chave = re.sub(r"[\s_]+", "", col.strip().upper())
            
            if chave == "CNPJ":
                mapping[col] = "CNPJ"
            elif chave in ("RAZAOSOCIAL", "RAZAOSOCIALOPERADORA"):
                mapping[col] = "RAZAO_SOCIAL"
            elif chave in ("VALORDEDESPESAS", "VALORTRIMESTRE"):
                mapping[col] = "VALOR_DE_DESPESAS"
            elif chave == "TRIMESTRE":
                mapping[col] = "TRIMESTRE"
            elif chave == "ANO":
                mapping[col] = "ANO"
            elif chave in ("REGISTROANS", "REGANS", "REANS", "REG.ANS"):
                mapping[col] = "REGISTROANS"
            elif chave == "MODALIDADE":
                mapping[col] = "MODALIDADE"
            elif chave == "UF":
                mapping[col] = "UF"
            elif chave == "DESCRICAO":
                mapping[col] = "DESCRICAO"
        
        df.rename(columns=mapping, inplace=True)
        return df
    
    @staticmethod
    def parse_valor(valor) -> Optional[float]:
        """Converte valor de string (formato BR) para float.
        
        Args:
            valor: Valor a converter (pode ser string "1.234,56" ou número)
            
        Returns:
            Float ou None se inválido
        """
        if pd.isna(valor):
            return None
        
        try:
            if isinstance(valor, str):
                valor = valor.strip()
                if valor == "":
                    return None
                # Remove separador de milhares (.) e converte , em .
                valor = valor.replace(".", "").replace(",", ".")
                return float(valor)
            return float(valor)
        except Exception:
            return None
