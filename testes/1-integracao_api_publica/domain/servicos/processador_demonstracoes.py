"""Serviço de Domínio: Processamento de Demonstrações Contábeis.

Contém regras de negócio para:
- Agregação de operadoras (tratamento de duplicidade)
- Aplicação de regras de validação após JOIN
- Filtros de sinistros (com/sem deduções)
- Cálculo de valores de trimestre
- Filtros de despesas
- Detecção de erros de JOIN
"""

import pandas as pd
import numpy as np
from typing import Dict, Set, List
from infraestrutura.logger import get_logger

logger = get_logger("ProcessadorDemonstracoes")


class ProcessadorDemonstracoes:
    """Processa demonstrações contábeis aplicando regras de negócio."""
    
    @staticmethod
    def agregar_operadoras(df_operadoras: pd.DataFrame) -> pd.DataFrame:
        """Agrega operadoras tratando duplicidade e priorizando ativas.
        
        Regras:
        - Se houver duplicatas, contar total e quantidade de ativas
        - Se tiver exatamente 1 ativa, usar dados da ativa
        - Se não tiver ativa ou tiver múltiplas ativas, usar primeiro registro
        
        Args:
            df_operadoras: DataFrame com operadoras (deve ter colunas: REG_ANS, STATUS, CNPJ, RAZAO_SOCIAL, MODALIDADE, UF)
            
        Returns:
            DataFrame agregado com colunas adicionais para tratamento de duplicidade
        """
        # Garantir que REG_ANS é Int64
        if 'REG_ANS' not in df_operadoras.columns:
            logger.error("Coluna REG_ANS não encontrada")
            return pd.DataFrame()
        
        df_operadoras['REG_ANS'] = pd.to_numeric(
            df_operadoras['REG_ANS'], 
            errors='coerce'
        ).astype('Int64')
        
        # Normalizar STATUS para uppercase
        df_operadoras['STATUS'] = df_operadoras['STATUS'].str.upper()
        
        # Agregar contando duplicatas e separando dados de ativas
        df_agg = df_operadoras.groupby('REG_ANS').agg(
            qtd_operadoras=('REG_ANS', 'count'),
            qtd_ativas=('STATUS', lambda x: (x == 'ATIVA').sum()),
            
            # Dados da operadora ATIVA (quando houver exatamente 1 ativa)
            cnpj_ativo=('CNPJ', lambda x: x[df_operadoras.loc[x.index, 'STATUS'] == 'ATIVA'].iloc[0] 
                        if any(df_operadoras.loc[x.index, 'STATUS'] == 'ATIVA') else None),
            razao_social_ativa=('RAZAO_SOCIAL', lambda x: x[df_operadoras.loc[x.index, 'STATUS'] == 'ATIVA'].iloc[0] 
                                if any(df_operadoras.loc[x.index, 'STATUS'] == 'ATIVA') else None),
            modalidade_ativa=('MODALIDADE', lambda x: x[df_operadoras.loc[x.index, 'STATUS'] == 'ATIVA'].iloc[0] 
                              if any(df_operadoras.loc[x.index, 'STATUS'] == 'ATIVA') else None),
            uf_ativo=('UF', lambda x: x[df_operadoras.loc[x.index, 'STATUS'] == 'ATIVA'].iloc[0] 
                      if any(df_operadoras.loc[x.index, 'STATUS'] == 'ATIVA') else None),
            status_ativo=('STATUS', lambda x: x[df_operadoras.loc[x.index, 'STATUS'] == 'ATIVA'].iloc[0] 
                          if any(df_operadoras.loc[x.index, 'STATUS'] == 'ATIVA') else None),
            
            # Dados do primeiro registro (fallback)
            cnpj=('CNPJ', 'first'),
            razao_social=('RAZAO_SOCIAL', 'first'),
            modalidade=('MODALIDADE', 'first'),
            uf=('UF', 'first'),
            status=('STATUS', 'first'),
        ).reset_index()
        
        logger.info(f"Operadoras agregadas: {len(df_agg)} registros únicos")
        logger.info(f"  - Com duplicatas: {(df_agg['qtd_operadoras'] > 1).sum()}")
        logger.info(f"  - Únicas: {(df_agg['qtd_operadoras'] == 1).sum()}")
        
        return df_agg
    
    @staticmethod
    def aplicar_regras_duplicidade(df_merged: pd.DataFrame) -> pd.DataFrame:
        """Aplica regras de negócio para tratar duplicidade de operadoras após JOIN.
        
        Regras:
        - Se REG_ANS não encontrado (missing): usar 'N/L' ou 'NAO_LOCALIZADO'
        - Se duplicado com exatamente 1 ativa: usar dados da ativa
        - Se duplicado com 0 ou múltiplas ativas: marcar como 'DUPLICIDADE'
        - Se único: usar dados normais
        
        Args:
            df_merged: DataFrame após merge com operadoras agregadas
                       (deve ter colunas: REG_ANS, qtd_operadoras, qtd_ativas, 
                        cnpj, cnpj_ativo, razao_social, razao_social_ativa, etc.)
        
        Returns:
            DataFrame com colunas finais aplicando as regras de duplicidade
        """
        # Criar máscaras para diferentes cenários
        reg_ans_missing = df_merged['REG_ANS'].isna()
        dup = df_merged['qtd_operadoras'] > 1
        dup_one_active = dup & (df_merged['qtd_ativas'] == 1)
        dup_other = dup & ~dup_one_active
        
        # Aplicar regras vetorizadas usando np.where
        df_merged['cnpj'] = np.where(
            reg_ans_missing,
            'N/L',
            np.where(
                dup_one_active,
                df_merged['cnpj_ativo'].fillna('N/L'),
                np.where(
                    dup_other,
                    'REGISTRO DE OPERADORA EM DUPLICIDADE',
                    df_merged['cnpj'].fillna('N/L')
                )
            )
        )
        
        df_merged['razao_social_operadora'] = np.where(
            reg_ans_missing,
            'N/L',
            np.where(
                dup_one_active,
                df_merged['razao_social_ativa'].fillna('N/L'),
                np.where(
                    dup_other,
                    'REGISTRO DE OPERADORA EM DUPLICIDADE',
                    df_merged['razao_social'].fillna('N/L')
                )
            )
        )
        
        df_merged['modalidade'] = np.where(
            reg_ans_missing,
            'N/L',
            np.where(
                dup_one_active,
                df_merged['modalidade_ativa'].fillna('N/L'),
                np.where(
                    dup_other,
                    'N/L',
                    df_merged['modalidade'].fillna('N/L')
                )
            )
        )
        
        df_merged['uf'] = np.where(
            reg_ans_missing,
            'N/L',
            np.where(
                dup_one_active,
                df_merged['uf_ativo'].fillna('N/L'),
                np.where(
                    dup_other,
                    'N/L',
                    df_merged['uf'].fillna('N/L')
                )
            )
        )
        
        df_merged['status_operadora'] = np.where(
            reg_ans_missing,
            'NAO_LOCALIZADO',
            np.where(
                dup_one_active,
                df_merged['status_ativo'].fillna('ATIVO'),
                np.where(
                    dup_other,
                    'REGISTRO DE OPERADORA EM DUPLICIDADE',
                    df_merged['status'].fillna('DESCONHECIDO')
                )
            )
        )
        
        logger.info("Regras de duplicidade aplicadas")
        return df_merged
    
    @staticmethod
    def detectar_erros_join(df: pd.DataFrame) -> List[Dict]:
        """Detecta registros com problemas no JOIN (operadora não localizada ou duplicada).
        
        Args:
            df: DataFrame com coluna 'razao_social_operadora'
            
        Returns:
            Lista de dicionários com informações dos erros
        """
        erros_join = []
        mascara_erro = df['razao_social_operadora'].isin(['N/L', 'REGISTRO DE OPERADORA EM DUPLICIDADE'])
        
        if mascara_erro.any():
            df_erros = df.loc[mascara_erro]
            for _, linha in df_erros.iterrows():
                motivo = 'OPERADORA_NAO_LOCALIZADA' if linha['razao_social_operadora'] == 'N/L' else 'OPERADORA_DUPLICADA'
                erros_join.append({
                    'arquivo_origem': 'JOIN_CONSOLIDADO',
                    'linha_arquivo': None,
                    'reg_ans': linha.get('reg_ans'),
                    'cd_conta_contabil': linha.get('cd_conta_contabil'),
                    'descricao': linha.get('descricao'),
                    'vl_saldo_inicial': linha.get('vl_saldo_inicial'),
                    'vl_saldo_final': linha.get('vl_saldo_final'),
                    'trimestre': linha.get('trimestre'),
                    'ano': linha.get('ano'),
                    'motivo_erro': motivo,
                    'tipo_erro': 'JOIN_OPERADORA',
                    'origem': 'Consolidação via JOIN (Python)'
                })
            
            logger.warning(f"JOIN com {len(erros_join)} registros com operadora N/L ou DUPLICIDADE")
        
        return erros_join
    
    @staticmethod
    def agregar_sinistros_sem_deducoes(
        df: pd.DataFrame,
        colunas_agrupamento: List[str] = None
    ) -> pd.DataFrame:
        """Agrega sinistros sem deduções somando valor_trimestre por grupo.
        
        Args:
            df: DataFrame com sinistros sem deduções
            colunas_agrupamento: Colunas para agrupar (padrão: reg_ans, cnpj, razao_social_operadora, trimestre, ano)
            
        Returns:
            DataFrame agregado com soma de valor_trimestre
        """
        if df.empty:
            return df
        
        if colunas_agrupamento is None:
            colunas_agrupamento = ['reg_ans', 'cnpj', 'razao_social_operadora', 'trimestre', 'ano']
        
        # Verificar se colunas existem
        colunas_existentes = [col for col in colunas_agrupamento if col in df.columns]
        
        if not colunas_existentes:
            logger.warning("Nenhuma coluna de agrupamento encontrada")
            return df
        
        df_agrupado = df.groupby(
            colunas_existentes,
            as_index=False
        ).agg({
            'valor_trimestre': 'sum'
        })
        
        logger.info(f"Sinistros agregados: {len(df)} registros → {len(df_agrupado)} grupos")
        return df_agrupado
    
    @staticmethod
    def preparar_csv_sinistros_com_deducoes(df_sinistros_br: pd.DataFrame) -> pd.DataFrame:
        """Prepara DataFrame de sinistros COM deduções para CSV de saída.
        
        Regras:
        - Selecionar colunas específicas na ordem definida
        - Renomear colunas para formato de saída
        
        Args:
            df_sinistros_br: DataFrame normalizado para formato brasileiro
            
        Returns:
            DataFrame formatado para CSV de saída
        """
        colunas_selecionadas = [
            'cnpj',
            'razao_social_operadora',
            'trimestre',
            'ano',
            'valor_trimestre',
            'reg_ans',
            'cd_conta_contabil',
            'descricao'
        ]
        
        # Verificar se todas as colunas existem
        colunas_existentes = [col for col in colunas_selecionadas if col in df_sinistros_br.columns]
        
        df_saida = df_sinistros_br[colunas_existentes].rename(columns={
            'cnpj': 'CNPJ',
            'razao_social_operadora': 'RAZAOSOCIAL',
            'trimestre': 'TRIMESTRE',
            'ano': 'ANO',
            'valor_trimestre': 'VALOR DE DESPESAS',
            'reg_ans': 'REGISTRO ANS',
            'cd_conta_contabil': 'CONTA CONTÁBIL',
            'descricao': 'DESCRICAO'
        })
        
        logger.debug(f"CSV sinistros com deduções preparado: {len(df_saida)} registros")
        return df_saida
    
    @staticmethod
    def preparar_csv_sinistros_sem_deducoes(df_agrupado_br: pd.DataFrame) -> pd.DataFrame:
        """Prepara DataFrame de sinistros SEM deduções para CSV de saída.
        
        Regras:
        - Selecionar colunas específicas na ordem definida
        - Renomear colunas para formato de saída
        - Garantir ordenação por ANO, TRIMESTRE, REG. ANS, CNPJ
        
        Args:
            df_agrupado_br: DataFrame agregado normalizado para formato brasileiro
            
        Returns:
            DataFrame formatado e ordenado para CSV de saída
        """
        colunas_selecionadas = [
            'reg_ans',
            'cnpj',
            'razao_social_operadora',
            'trimestre',
            'ano',
            'valor_trimestre'
        ]
        
        # Verificar se todas as colunas existem
        colunas_existentes = [col for col in colunas_selecionadas if col in df_agrupado_br.columns]
        
        df_saida = df_agrupado_br[colunas_existentes].rename(columns={
            'reg_ans': 'REG. ANS',
            'cnpj': 'CNPJ',
            'razao_social_operadora': 'RAZAOSOCIAL',
            'trimestre': 'TRIMESTRE',
            'ano': 'ANO',
            'valor_trimestre': 'VALOR DE DESPESAS'
        })
        
        # Garantir ordenação
        colunas_ordenacao = ['ANO', 'TRIMESTRE', 'REG. ANS', 'CNPJ']
        colunas_ordenacao_existentes = [col for col in colunas_ordenacao if col in df_saida.columns]
        
        if colunas_ordenacao_existentes:
            df_saida = df_saida.sort_values(
                colunas_ordenacao_existentes,
                kind='mergesort'
            )
        
        logger.debug(f"CSV sinistros sem deduções preparado: {len(df_saida)} registros")
        return df_saida
    
    @staticmethod
    def filtrar_sinistros_com_deducoes(df: pd.DataFrame) -> pd.DataFrame:
        """Filtra despesas com sinistros INCLUINDO deduções.
        
        Regras:
        - Linha principal: descrição contém "Despesas com Eventos" E "Sinistros"
        - Linha principal DEVE ter cd_conta_contabil com 9 dígitos começando com '4'
        - Deduções: linhas seguintes que começam com "-" ou "(-)"
        - Deduções devem ter cd_conta_contabil com 9 dígitos
        - Para quando encontrar linha que não atende critérios
        
        IMPORTANTE: DataFrame DEVE estar ordenado por ano, trimestre, reg_ans, cd_conta_contabil
        para que as deduções estejam nas linhas seguintes à linha principal.
        
        Args:
            df: DataFrame com demonstrações (deve ter: descricao, cd_conta_contabil)
            
        Returns:
            DataFrame filtrado com sinistros e deduções
        """
        # ORDENAR por ano, trimestre, reg_ans e cd_conta_contabil (CRÍTICO!)
        colunas_ordenacao = []
        if 'ano' in df.columns:
            colunas_ordenacao.append('ano')
        if 'trimestre' in df.columns:
            colunas_ordenacao.append('trimestre')
        if 'reg_ans' in df.columns:
            colunas_ordenacao.append('reg_ans')
        if 'cd_conta_contabil' in df.columns:
            colunas_ordenacao.append('cd_conta_contabil')
        
        if colunas_ordenacao:
            df = df.sort_values(colunas_ordenacao, kind='mergesort')
            logger.debug(f"DataFrame ordenado por: {', '.join(colunas_ordenacao)}")
        
        # Resetar índice para acesso sequencial
        df = df.reset_index(drop=True)
        
        # Máscaras vetorizadas - muito mais rápido que iterrows()
        descricao_str = df['descricao'].astype(str).str.strip()
        cd_conta_str = df['cd_conta_contabil'].astype(str).str.strip()
        
        # Linha principal de sinistros
        mascara_principal = (descricao_str.str.contains('Despesas com Eventos', na=False)) & \
                           (descricao_str.str.contains('Sinistros', na=False)) & \
                           (cd_conta_str.str.len() == 9) & \
                           (cd_conta_str.str.startswith('4'))
        
        # Deduções (começam com - ou (-)) E TAMBÉM devem ter conta começando com 4
        mascara_deducao = (descricao_str.str.startswith('-') | descricao_str.str.startswith('(-)')) & \
                         (cd_conta_str.str.len() == 9) & \
                         (cd_conta_str.str.startswith('4'))
        
        # Encontrar índices das linhas principais
        indices_principais = df[mascara_principal].index.tolist()
        indices_selecionados = set(indices_principais)
        
        # Para cada linha principal, adicionar deduções subsequentes
        for idx_principal in indices_principais:
            # Procurar deduções nas próximas linhas (limitar a 20 para performance)
            for offset in range(1, min(20, len(df) - idx_principal)):
                idx_prox = idx_principal + offset
                
                # Se for dedução, adicionar
                if mascara_deducao.iloc[idx_prox]:
                    indices_selecionados.add(idx_prox)
                else:
                    # Parar quando encontrar linha que não é dedução
                    break
        
        df_resultado = df.loc[sorted(indices_selecionados)]
        logger.info(f"Sinistros com deduções: {len(df_resultado)} registros")
        return df_resultado
    
    @staticmethod
    def filtrar_sinistros_sem_deducoes(df: pd.DataFrame) -> pd.DataFrame:
        """Filtra despesas com sinistros SEM deduções (apenas linhas principais).
        
        Regras:
        - Descrição contém "Despesas com Eventos" E "Sinistros"
        - cd_conta_contabil tem 9 dígitos E começa com '4'
        
        Args:
            df: DataFrame com demonstrações (deve ter: descricao, cd_conta_contabil)
            
        Returns:
            DataFrame filtrado apenas com linhas principais de sinistros
        """
        # Operação vetorizada - muito mais rápida que iterrows()
        mascara_descricao = df['descricao'].astype(str).str.contains('Despesas com Eventos', na=False) & \
                           df['descricao'].astype(str).str.contains('Sinistros', na=False)
        
        # Converter cd_conta para string e validar
        cd_conta_str = df['cd_conta_contabil'].astype(str).str.strip()
        mascara_conta = (cd_conta_str.str.len() == 9) & cd_conta_str.str.startswith('4')
        
        # Aplicar ambas as máscaras
        df_resultado = df[mascara_descricao & mascara_conta]
        
        logger.info(f"Sinistros sem deduções: {len(df_resultado)} registros")
        return df_resultado
    
    @staticmethod
    def filtrar_despesas(df: pd.DataFrame) -> pd.DataFrame:
        """Filtra apenas despesas (cd_conta_contabil começa com '4').
        
        Args:
            df: DataFrame com demonstrações (deve ter: cd_conta_contabil)
            
        Returns:
            DataFrame filtrado apenas com despesas
        """
        df_resultado = df[df['cd_conta_contabil'].astype(str).str.startswith('4', na=False)]
        logger.info(f"Despesas filtradas: {len(df_resultado)} registros")
        return df_resultado
    
    @staticmethod
    def remover_valores_zero(df: pd.DataFrame, coluna_valor: str = 'valor_trimestre') -> pd.DataFrame:
        """Remove registros com valor de despesas igual a zero.
        
        Args:
            df: DataFrame com demonstrações
            coluna_valor: Nome da coluna com valores a verificar
            
        Returns:
            DataFrame sem registros de valor zero
        """
        if df.empty:
            return df
        
        qtd_antes = len(df)
        df_resultado = df[df[coluna_valor].fillna(0) != 0]
        qtd_removidos = qtd_antes - len(df_resultado)
        
        if qtd_removidos > 0:
            logger.info(f"Removidos {qtd_removidos} registros com {coluna_valor}=0")
        
        return df_resultado
    
    @staticmethod
    def calcular_valor_trimestre(df: pd.DataFrame) -> pd.DataFrame:
        """Calcula valor_trimestre = vl_saldo_final - vl_saldo_inicial.
        
        Args:
            df: DataFrame com demonstrações (deve ter: vl_saldo_inicial, vl_saldo_final)
            
        Returns:
            DataFrame com coluna valor_trimestre calculada
        """
        if 'vl_saldo_inicial' in df.columns and 'vl_saldo_final' in df.columns:
            df['valor_trimestre'] = df['vl_saldo_final'] - df['vl_saldo_inicial']
            logger.debug("Coluna valor_trimestre calculada")
        
        return df
    
    @staticmethod
    def aplicar_pipeline_sinistros(
        df: pd.DataFrame,
        com_deducoes: bool = True
    ) -> Dict[str, pd.DataFrame]:
        """Pipeline completo para processar sinistros.
        
        Args:
            df: DataFrame com demonstrações
            com_deducoes: Se True, filtra com deduções; se False, sem deduções
            
        Returns:
            Dict com:
                - 'resultado': DataFrame processado
                - 'estatisticas': Dict com estatísticas do processamento
        """
        estatisticas = {
            'total_original': len(df),
            'apos_filtro_sinistros': 0,
            'apos_remover_zeros': 0,
            'removidos_zero': 0
        }
        
        # Filtrar sinistros
        if com_deducoes:
            df_sinistros = ProcessadorDemonstracoes.filtrar_sinistros_com_deducoes(df)
        else:
            df_sinistros = ProcessadorDemonstracoes.filtrar_sinistros_sem_deducoes(df)
        
        estatisticas['apos_filtro_sinistros'] = len(df_sinistros)
        
        # Remover valores zero
        df_final = ProcessadorDemonstracoes.remover_valores_zero(df_sinistros)
        estatisticas['apos_remover_zeros'] = len(df_final)
        estatisticas['removidos_zero'] = estatisticas['apos_filtro_sinistros'] - estatisticas['apos_remover_zeros']
        
        return {
            'resultado': df_final,
            'estatisticas': estatisticas
        }
