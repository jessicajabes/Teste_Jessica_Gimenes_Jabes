"""Serviço de Domínio para Geração de Consolidados.

Contém toda a lógica de negócio relacionada a geração
de arquivos consolidados e relatórios.
"""

import os
import pandas as pd
import zipfile
import hashlib
import json
from typing import Dict
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

from infraestrutura.logger import get_logger

logger = get_logger('GeradorConsolidados')


class GeradorConsolidados:
    """Lógica de negócio para gerar consolidados e relatórios."""
    
    # Cache para armazenar hashes de DataFrames já processados
    CACHE_DIR = Path(os.path.expanduser("~/.cache/teste_jessica"))
    
    @staticmethod
    def _calcular_hash_dataframe(df: pd.DataFrame) -> str:
        """Calcula hash MD5 do DataFrame para cache."""
        df_str = pd.util.hash_pandas_object(df, index=True).values.tobytes()
        return hashlib.md5(df_str).hexdigest()
    
    @staticmethod
    def _obter_cache_consolidado(nome_arquivo: str, df_hash: str) -> str | None:
        """Verifica se há cache válido para este DataFrame."""
        cache_file = GeradorConsolidados.CACHE_DIR / f"{nome_arquivo}_{df_hash}.json"
        if cache_file.exists():
            logger.debug(f"Cache encontrado para {nome_arquivo}")
            return str(cache_file)
        return None
    
    @staticmethod
    def _salvar_cache_consolidado(nome_arquivo: str, df_hash: str, caminho_saida: str) -> None:
        """Salva informação de cache após gerar consolidado."""
        GeradorConsolidados.CACHE_DIR.mkdir(parents=True, exist_ok=True)
        cache_file = GeradorConsolidados.CACHE_DIR / f"{nome_arquivo}_{df_hash}.json"
        with open(cache_file, 'w') as f:
            json.dump({"caminho": caminho_saida, "timestamp": os.path.getmtime(caminho_saida)}, f)
    
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
        aplicar_ordenacao: bool = True,
        usar_cache: bool = True,
        tamanho_chunk: int = 10000
    ) -> bool:
        """Gera um CSV consolidado com formatação, ordenação e cache opcional.
        
        Escreve em chunks para reduzir uso de memória durante serialização.
        A opção usar_cache permite pular regeneração de consolidados já processados.
        
        Args:
            df: DataFrame a processar
            caminho_saida: Caminho do arquivo CSV de saída
            aplicar_formatacao_br: Aplicar formatação brasileira
            aplicar_ordenacao: Aplicar ordenação padrão
            usar_cache: Usar cache para pular regenerações
            tamanho_chunk: Tamanho de cada chunk para escrita (padrão 10k linhas)
        """
        try:
            # Verificar cache
            nome_arquivo = os.path.basename(caminho_saida)
            if usar_cache:
                df_hash = GeradorConsolidados._calcular_hash_dataframe(df)
                cache_path = GeradorConsolidados._obter_cache_consolidado(nome_arquivo, df_hash)
                if cache_path:
                    logger.info(f"Usando cache para {nome_arquivo} (pulando regeneração)")
                    return True
            
            df_processado = df.copy()
            
            if aplicar_ordenacao:
                df_processado = GeradorConsolidados.aplicar_ordenacao_padrao(df_processado)
            
            if aplicar_formatacao_br:
                df_processado = GeradorConsolidados.normalizar_para_br(df_processado)
            
            os.makedirs(os.path.dirname(caminho_saida), exist_ok=True)
            
            # Escrita em chunks para reduzir pico de memória
            num_chunks = (len(df_processado) + tamanho_chunk - 1) // tamanho_chunk
            
            with open(caminho_saida, 'w', encoding='utf-8', newline='') as f:
                for i, chunk_idx in enumerate(range(0, len(df_processado), tamanho_chunk)):
                    chunk = df_processado.iloc[chunk_idx:chunk_idx + tamanho_chunk]
                    chunk.to_csv(
                        f, 
                        sep=';', 
                        index=False, 
                        encoding='utf-8',
                        header=(i == 0)  # Header apenas no primeiro chunk
                    )
            
            # Salvar cache
            if usar_cache:
                df_hash = GeradorConsolidados._calcular_hash_dataframe(df)
                GeradorConsolidados._salvar_cache_consolidado(nome_arquivo, df_hash, caminho_saida)
            
            logger.info(f"CSV consolidado gerado em {num_chunks} chunks: {caminho_saida}")
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

    @staticmethod
    def gerar_multiplos_consolidados_paralelo(
        consolidados: Dict[str, pd.DataFrame],
        diretorio_saida: str,
        aplicar_formatacao_br: bool = True,
        aplicar_ordenacao: bool = True,
        usar_cache: bool = True,
        max_workers: int = 3
    ) -> Dict[str, bool]:
        """Gera múltiplos consolidados em paralelo para melhor performance.
        
        Args:
            consolidados: Dict com {nome_arquivo: DataFrame}
            diretorio_saida: Diretório onde salvar
            aplicar_formatacao_br: Aplicar formatação brasileira
            aplicar_ordenacao: Aplicar ordenação padrão
            usar_cache: Usar cache para pular regenerações
            max_workers: Máximo de threads paralelas (padrão 3)
        
        Returns:
            Dict com {nome_arquivo: sucesso_bool}
        """
        resultados = {}
        os.makedirs(diretorio_saida, exist_ok=True)
        
        def gerar_consolidado(args):
            nome_arquivo, df = args
            caminho_saida = os.path.join(diretorio_saida, f"{nome_arquivo}.csv")
            return nome_arquivo, GeradorConsolidados.gerar_csv_consolidado(
                df,
                caminho_saida,
                aplicar_formatacao_br=aplicar_formatacao_br,
                aplicar_ordenacao=aplicar_ordenacao,
                usar_cache=usar_cache
            )
        
        try:
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                futures = {
                    executor.submit(gerar_consolidado, (nome, df)): nome 
                    for nome, df in consolidados.items()
                }
                
                for future in as_completed(futures):
                    nome, sucesso = future.result()
                    resultados[nome] = sucesso
                    status = "✓" if sucesso else "✗"
                    logger.info(f"{status} Consolidado '{nome}' processado")
            
            logger.info(f"Todos os {len(resultados)} consolidados processados (paralelo)")
            return resultados
            
        except Exception as e:
            logger.error(f"Erro ao gerar consolidados em paralelo: {e}")
            return {nome: False for nome in consolidados.keys()}
