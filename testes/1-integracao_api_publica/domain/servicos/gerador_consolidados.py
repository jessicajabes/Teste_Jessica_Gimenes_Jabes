"""Serviço de Domínio para Geração de Consolidados.

Contém toda a lógica de negócio relacionada a geração
de arquivos consolidados e relatórios.
"""

import os
import pandas as pd
import zipfile
from typing import Dict

from infraestrutura.logger import get_logger

logger = get_logger('GeradorConsolidados')


class GeradorConsolidados:
    """Lógica de negócio para gerar consolidados e relatórios."""
    
    @staticmethod
    def normalizar_para_br(df: pd.DataFrame) -> pd.DataFrame:
        """Converte valores numéricos para formato brasileiro (1.234,56)."""
        df_br = df.copy()
        
        for col in df_br.columns:
            if df_br[col].dtype in ['float64', 'float32']:
                df_br[col] = df_br[col].apply(
                    lambda x: f"{x:,.2f}".replace(',', '#').replace('.', ',').replace('#', '.')
                    if pd.notna(x) else x
                )
        
        return df_br
    
    @staticmethod
    def aplicar_ordenacao_padrao(df: pd.DataFrame) -> pd.DataFrame:
        """Aplica a ordenação padrão: ano, trimestre, reg_ans, cd_conta_contabil."""
        colunas_ordem = ['ano', 'trimestre', 'reg_ans', 'cd_conta_contabil']
        colunas_existentes = [col for col in colunas_ordem if col in df.columns]
        
        if colunas_existentes:
            df = df.sort_values(colunas_existentes, kind='mergesort')
        
        return df
    
    @staticmethod
    def gerar_csv_consolidado(
        df: pd.DataFrame,
        caminho_saida: str,
        aplicar_formatacao_br: bool = True,
        aplicar_ordenacao: bool = True
    ) -> bool:
        """Gera um CSV consolidado com formatação e ordenação opcional."""
        try:
            df_processado = df.copy()
            
            if aplicar_ordenacao:
                df_processado = GeradorConsolidados.aplicar_ordenacao_padrao(df_processado)
            
            if aplicar_formatacao_br:
                df_processado = GeradorConsolidados.normalizar_para_br(df_processado)
            
            os.makedirs(os.path.dirname(caminho_saida), exist_ok=True)
            df_processado.to_csv(caminho_saida, sep=';', index=False, encoding='utf-8')
            
            logger.info(f"CSV consolidado gerado: {caminho_saida}")
            return True
            
        except Exception as e:
            logger.error(f"Erro ao gerar CSV consolidado: {e}")
            return False
    
    @staticmethod
    def criar_zip_consolidado(
        arquivos: Dict[str, str],
        caminho_zip: str,
        arquivos_logs: Dict[str, str] = None
    ) -> bool:
        """Cria um arquivo ZIP com múltiplos CSVs e logs opcionais.
        
        Args:
            arquivos: Dict com {nome_no_zip: caminho_arquivo}
            caminho_zip: Caminho onde o ZIP será salvo
            arquivos_logs: Dict opcional com {nome_no_zip: caminho_log}
        """
        try:
            os.makedirs(os.path.dirname(caminho_zip), exist_ok=True)
            
            with zipfile.ZipFile(caminho_zip, 'w', zipfile.ZIP_DEFLATED) as zipf:
                # Adicionar CSVs
                for nome, caminho in arquivos.items():
                    if os.path.exists(caminho):
                        zipf.write(caminho, nome)
                        logger.debug(f"Arquivo adicionado ao ZIP: {nome}")
                
                # Adicionar logs se fornecidos
                if arquivos_logs:
                    for nome, caminho in arquivos_logs.items():
                        if os.path.exists(caminho):
                            zipf.write(caminho, f"logs/{nome}")
                            logger.debug(f"Log adicionado ao ZIP: logs/{nome}")
            
            logger.info(f"ZIP consolidado criado: {caminho_zip}")
            return True
            
        except Exception as e:
            logger.error(f"Erro ao criar ZIP consolidado: {e}")
            return False
    
    @staticmethod
    def exibir_resumo_consolidacao(
        diretorio_saida: str,
        total_registros: int,
        total_erros: int,
        valor_inicial: float,
        valor_final: float
    ) -> None:
        """Exibe um resumo formatado da consolidação."""
        print(f"\n{'='*60}")
        print("PROCESSAMENTO CONCLUÍDO")
        print(f"{'='*60}")
        print(f"Total de registros carregados: {total_registros}")
        print(f"Total de erros: {total_erros}")
        print(f"Arquivos gerados em: {os.path.join(diretorio_saida, 'consolidados')}/")
        print("  • consolidado_despesas_sinistros.csv")
        print("  • consolidado_todas_despesas.csv")
        print("  • consolidado_despesas.zip")
        
        print(f"\n📊 COMPARATIVO DE VALORES")
        print(f"{'='*60}")
        print(
            f"Valor Total Inicial (arquivos brutos): R$ {valor_inicial:,.2f}"
            .replace(',', '#')
            .replace('.', ',')
            .replace('#', '.')
        )
        print(
            f"Valor Total Final (CSV gerado):        R$ {valor_final:,.2f}"
            .replace(',', '#')
            .replace('.', ',')
            .replace('#', '.')
        )
        diferenca = valor_inicial - valor_final
        percentual = (diferenca / valor_inicial * 100) if valor_inicial != 0 else 0
        print(
            f"Diferença:                              R$ {diferenca:,.2f}"
            .replace(',', '#')
            .replace('.', ',')
            .replace('#', '.')
        )
        print(f"Percentual:                             {percentual:.2f}%")
        print(f"{'='*60}\n")
