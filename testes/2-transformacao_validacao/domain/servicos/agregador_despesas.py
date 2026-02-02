"""
Serviço responsável pela agregação de dados de despesas.
Centraliza a lógica de agrupamento e cálculo de estatísticas.
"""
from typing import Optional
import pandas as pd


class AgregadorDespesas:
    """Agrega e calcula estatísticas de despesas"""

    @staticmethod
    def agregar_por_operadora_uf(df: pd.DataFrame) -> Optional[pd.DataFrame]:
        """
        Agrega despesas por operadora e UF, calculando estatísticas.
        
        Args:
            df: DataFrame com despesas validadas
        
        Returns:
            DataFrame agregado ou None se vazio
        """
        if df.empty:
            return None

        df_valid = df[df["VALOR_NUM"].notna()].copy()
        if df_valid.empty:
            return None

        df_valid["UF"] = df_valid["UF"].fillna("N/L")
        if "REGISTROANS" not in df_valid.columns:
            df_valid["REGISTROANS"] = "N/L"

        # Agregação base
        base = df_valid.groupby(["RAZAO_SOCIAL", "UF", "REGISTROANS"], dropna=False).agg(
            total_despesas=("VALOR_NUM", "sum"),
            qtd_registros=("VALOR_NUM", "size"),
            qtd_trimestres=("TRIMESTRE", "nunique"),
            qtd_anos=("ANO", "nunique"),
        ).reset_index()

        # Estatísticas por trimestre
        por_trimestre = df_valid.groupby(
            ["RAZAO_SOCIAL", "UF", "REGISTROANS", "TRIMESTRE"], 
            dropna=False
        )["VALOR_NUM"].sum().reset_index()
        
        # Calcular desvio padrão dos valores dos trimestres com dados
        stats = por_trimestre.groupby(["RAZAO_SOCIAL", "UF", "REGISTROANS"], dropna=False)["VALOR_NUM"].agg(
            desvio_padrao_despesas="std",
        ).reset_index()

        # Merge e finalização
        resultado = base.merge(stats, on=["RAZAO_SOCIAL", "UF", "REGISTROANS"], how="left")
        resultado["desvio_padrao_despesas"] = resultado["desvio_padrao_despesas"].fillna(0)
        
        # Calcular média por trimestre (total / qtd_trimestres)
        resultado["media_despesas_trimestre"] = resultado["total_despesas"] / resultado["qtd_trimestres"]
        
        # Arredondar todos os valores para 2 casas decimais
        resultado["total_despesas"] = resultado["total_despesas"].round(2)
        resultado["media_despesas_trimestre"] = resultado["media_despesas_trimestre"].round(2)
        resultado["desvio_padrao_despesas"] = resultado["desvio_padrao_despesas"].round(2)
        
        resultado = resultado.sort_values("total_despesas", ascending=False)

        # Renomear colunas para formato final
        resultado.rename(columns={
            "RAZAO_SOCIAL": "razao_social",
            "UF": "uf",
            "REGISTROANS": "reg_ans",
        }, inplace=True)

        return resultado[[
            "razao_social",
            "reg_ans",
            "uf",
            "total_despesas",
            "media_despesas_trimestre",
            "desvio_padrao_despesas",
            "qtd_registros",
            "qtd_trimestres",
            "qtd_anos",
        ]]

    @staticmethod
    def salvar_agregado(df: pd.DataFrame, caminho_saida: str):
        """
        Salva DataFrame agregado em CSV com formato brasileiro.
        
        Args:
            df: DataFrame agregado
            caminho_saida: Caminho do arquivo de saída
        """
        df_formato = df.copy()
        
        # Colunas numéricas que devem ser formatadas para o padrão brasileiro
        colunas_numericas = ["total_despesas", "media_despesas_trimestre", "desvio_padrao_despesas"]
        
        def formatar_moeda_brasileira(valor):
            """Formata valor como moeda brasileira: 1234567.89 -> 1.234.567,89"""
            if pd.isna(valor):
                return ""
            # Formata com 2 casas decimais e separador de milhares (,)
            # Depois inverte pontos e vírgulas para padrão brasileiro
            return f"{valor:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
        
        for col in colunas_numericas:
            if col in df_formato.columns:
                df_formato[col] = df_formato[col].apply(formatar_moeda_brasileira)
        
        df_formato.to_csv(caminho_saida, index=False, encoding="utf-8-sig", sep=";")
