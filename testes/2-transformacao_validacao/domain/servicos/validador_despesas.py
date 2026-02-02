"""
Serviço responsável pela validação e enriquecimento de dados de despesas.
Centraliza toda a lógica de validação de campos obrigatórios, valores, CNPJs e enriquecimento.
"""
import re
import logging
import pandas as pd

from .validador_cnpj import ValidadorCNPJ
from .enriquecedor_operadoras import EnriquecedorOperadoras
from .normalizador_dados import NormalizadorDados


class ValidadorDespesas:
    """Valida e enriquece dados de despesas"""

    @staticmethod
    def validar_e_enriquecer(
        df: pd.DataFrame,
        operadoras: pd.DataFrame,
        nome_base: str,
        logger: logging.Logger,
    ) -> pd.DataFrame:
        """
        Valida e enriquece DataFrame de despesas com todas as regras de negócio.
        
        Args:
            df: DataFrame com despesas
            operadoras: DataFrame com operadoras
            nome_base: Nome base do arquivo (para logs)
            logger: Logger para registrar validações
        
        Returns:
            DataFrame validado e enriquecido
        """
        df = df.copy()
        df = NormalizadorDados.normalizar_colunas(df)

        # Validar colunas obrigatórias
        df = ValidadorDespesas._validar_colunas_obrigatorias(df, nome_base, logger)
        if df.empty:
            return df

        # Validar razão social
        df = ValidadorDespesas._validar_razao_social(df, nome_base, logger)

        # Validar valores numéricos
        df = ValidadorDespesas._validar_valores_numericos(df, nome_base, logger)

        # Validar CNPJs
        df = ValidadorDespesas._validar_cnpjs(df, nome_base, logger)

        # Enriquecer com MODALIDADE e UF
        mapa_reg_ans = EnriquecedorOperadoras.criar_mapa_por_registro_ans(operadoras, logger=logger)
        df = EnriquecedorOperadoras.enriquecer_com_modalidade_uf(
            df, 
            mapa_reg_ans, 
            logger=logger, 
            nome_base=nome_base
        )

        return df

    @staticmethod
    def _validar_colunas_obrigatorias(
        df: pd.DataFrame,
        nome_base: str,
        logger: logging.Logger,
    ) -> pd.DataFrame:
        """Valida presença de colunas obrigatórias"""
        colunas_obrigatorias = ["CNPJ", "RAZAO_SOCIAL", "VALOR_DE_DESPESAS", "TRIMESTRE", "ANO"]
        faltantes = [c for c in colunas_obrigatorias if c not in df.columns]
        
        if faltantes:
            logger.error(f"{nome_base}: colunas obrigatórias ausentes: {', '.join(faltantes)}")
            return pd.DataFrame()
        
        return df

    @staticmethod
    def _validar_razao_social(
        df: pd.DataFrame,
        nome_base: str,
        logger: logging.Logger,
    ) -> pd.DataFrame:
        """Valida razão social e preenche vazios com 'N/L'"""
        razao_series = df["RAZAO_SOCIAL"]
        mascara_razao_vazia = (
            razao_series.isna()
            | razao_series.astype(str).str.strip().str.lower().isin(["", "nan", "none"])
        )
        
        if mascara_razao_vazia.any():
            for idx in df[mascara_razao_vazia].index:
                logger.error(f"{nome_base}: RAZAO SOCIAL vazia na linha {idx + 1}")
            df.loc[mascara_razao_vazia, "RAZAO_SOCIAL"] = "N/L"
        
        return df

    @staticmethod
    def _validar_valores_numericos(
        df: pd.DataFrame,
        nome_base: str,
        logger: logging.Logger,
    ) -> pd.DataFrame:
        """Valida valores numéricos de despesas"""
        df["VALOR_NUM"] = df["VALOR_DE_DESPESAS"].apply(NormalizadorDados.parse_valor)
        
        # Validar valores inválidos
        mascara_valor_invalido = df["VALOR_NUM"].isna()
        if mascara_valor_invalido.any():
            for idx in df[mascara_valor_invalido].index:
                logger.error(f"{nome_base}: valor numérico inválido na linha {idx + 1}")

        # Verificar valores negativos (exceto deduções)
        if "DESCRICAO" in df.columns:
            descricao_series = df["DESCRICAO"].astype(str).str.strip()
            eh_deducao = descricao_series.str.startswith("-") | descricao_series.str.startswith("(-)")
            mascara_negativo = df["VALOR_NUM"].notna() & (df["VALOR_NUM"] < 0) & (~eh_deducao)
        else:
            mascara_negativo = df["VALOR_NUM"].notna() & (df["VALOR_NUM"] < 0)
        
        if mascara_negativo.any():
            for idx in df[mascara_negativo].index:
                logger.warning(f"{nome_base}: valor de despesa negativo na linha {idx + 1}")

        # Verificar se deduções têm valor positivo (deveriam ser negativas)
        if "DESCRICAO" in df.columns:
            mascara_deducao_positiva = df["VALOR_NUM"].notna() & (df["VALOR_NUM"] > 0) & eh_deducao
            if mascara_deducao_positiva.any():
                for idx in df[mascara_deducao_positiva].index:
                    logger.warning(f"{nome_base}: dedução com valor positivo na linha {idx + 1}")

        # Verificar valores zero
        mascara_zero = df["VALOR_NUM"].notna() & (df["VALOR_NUM"] == 0)
        if mascara_zero.any():
            for idx in df[mascara_zero].index:
                logger.warning(f"{nome_base}: valor de despesa igual a zero na linha {idx + 1}")

        return df

    @staticmethod
    def _validar_cnpjs(
        df: pd.DataFrame,
        nome_base: str,
        logger: logging.Logger,
    ) -> pd.DataFrame:
        """Valida CNPJs e adiciona colunas de validação"""
        df["CNPJ"] = df["CNPJ"].astype(str)
        cnpj_limp, cnpj_format_ok, cnpj_dv_ok = [], [], []
        
        for valor in df["CNPJ"]:
            limpo, formato_ok, dv_ok = ValidadorCNPJ.validar(valor)
            cnpj_limp.append(limpo)
            cnpj_format_ok.append(formato_ok)
            cnpj_dv_ok.append(dv_ok)

        df["CNPJ_LIMPO"] = cnpj_limp
        df["CNPJ_FORMATO_OK"] = cnpj_format_ok
        df["CNPJ_DV_OK"] = cnpj_dv_ok

        # Logar erros de CNPJ
        for idx, row in df.iterrows():
            cnpj_original = str(row['CNPJ']).strip()
            digitos_originais = re.sub(r"\D", "", cnpj_original)
            tamanho_original = len(digitos_originais)
            
            # Não gerar erro/aviso quando apenas normalizou (1-13 dígitos)
            if tamanho_original == 0 or tamanho_original >= 14:
                if not row["CNPJ_FORMATO_OK"]:
                    logger.error(f"{nome_base}: CNPJ com formato inválido na linha {idx + 1} ({row['CNPJ']})")
                if not row["CNPJ_DV_OK"]:
                    logger.error(f"{nome_base}: CNPJ com dígitos verificadores inválidos na linha {idx + 1} ({row['CNPJ']})")

        return df
