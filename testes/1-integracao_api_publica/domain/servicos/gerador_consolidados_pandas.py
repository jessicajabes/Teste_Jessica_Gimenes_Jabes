"""Serviço de Domínio: Gerador de Consolidados usando Pandas JOIN.

Responsável por:
1. Carregar operadoras ativas e canceladas dos CSVs
2. Consolidar operadoras (priorizar ativas)
3. Carregar despesas/sinistros dos CSVs extraídos
4. Fazer JOIN pandas entre despesas e operadoras
5. Consolidar todos os trimestres
6. Gerar arquivos finais (com/sem deduções) + ZIP

SEM uso de banco de dados - tudo em memória com pandas.
"""

import os
import zipfile
import pandas as pd
from typing import Dict
from datetime import datetime

from infraestrutura.logger import get_logger
from domain.servicos import ProcessadorDemonstracoes

logger = get_logger("GeradorConsolidadosPandas")


class GeradorConsolidadosPandas:
    """Gera arquivos consolidados usando pandas JOIN (sem banco de dados)."""
    
    def gerar_consolidados_com_join(
        self, 
        diretorio_origem: str,
        diretorio_destino: str,
        arquivo_log: str = None
    ) -> Dict:
        """Gera consolidados com JOIN pandas entre despesas e operadoras.
        
        Args:
            diretorio_origem: Diretório com arquivos extraídos
            diretorio_destino: Diretório para salvar consolidados
            arquivo_log: Caminho do arquivo de log da sessão
            
        Returns:
            Dict com resultado:
                - sucesso: bool
                - total_registros: int
                - com_operadora: int
                - sem_operadora: int
                - arquivos_gerados: List[str]
        """
        try:
            # 1. Carregar operadoras (ativas + canceladas)
            operadoras_df = self._carregar_operadoras_dataframe(diretorio_origem)
            if operadoras_df is None or operadoras_df.empty:
                return {
                    "sucesso": False,
                    "erro": "Nenhuma operadora encontrada",
                    "total_registros": 0,
                    "com_operadora": 0,
                    "sem_operadora": 0,
                    "arquivos_gerados": []
                }
            
            print(f"    ✓ {len(operadoras_df)} operadoras carregadas")
            
            # 2. Carregar todos os CSVs de trimestres
            arquivos_intermediarios = []
            todos_dados = []
            
            # Buscar todos os CSVs extraídos dos ZIPs
            csvs_encontrados = self._listar_csvs_extraidos(diretorio_origem)
            
            if not csvs_encontrados:
                return {
                    "sucesso": False,
                    "erro": "Nenhum arquivo CSV encontrado nos ZIPs extraídos",
                    "total_registros": 0,
                    "com_operadora": 0,
                    "sem_operadora": 0,
                    "arquivos_gerados": []
                }
            
            print(f"    ✓ {len(csvs_encontrados)} CSVs encontrados")
            
            # Processar cada CSV de trimestre e fazer JOIN
            for csv_path in csvs_encontrados:
                nome_csv = os.path.basename(csv_path)
                print(f"    Processando {nome_csv}...")
                
                despesas = self._carregar_despesas_do_caminho(csv_path)
                if despesas is None or despesas.empty:
                    print(f"      ⚠ Erro ao carregar {nome_csv}")
                    continue
                
                # Fazer JOIN
                resultado_join = self._fazer_join(despesas, operadoras_df)
                todos_dados.append(resultado_join)
            
            if not todos_dados:
                return {
                    "sucesso": False,
                    "erro": "Nenhum dado processado com sucesso",
                    "total_registros": 0,
                    "com_operadora": 0,
                    "sem_operadora": 0,
                    "arquivos_gerados": []
                }
            
            # 3. Consolidar todos os trimestres em um único DataFrame
            print("\n    Consolidando todos os trimestres...")
            df_consolidado = pd.concat(todos_dados, ignore_index=True)
            
            total = len(df_consolidado)
            com_operadora = (df_consolidado['RAZAO_SOCIAL'] != 'N/L').sum()
            sem_operadora = total - com_operadora
            
            print(f"    ✓ {total:,} registros consolidados ({com_operadora:,} com operadora)")
            
            # 4. Aplicar lógica de negócio do ProcessadorDemonstracoes
            print("\n    Gerando arquivos finais...")
            
            # Normalizar nomes de colunas para o formato esperado pelo ProcessadorDemonstracoes
            df_normalizado = self._normalizar_colunas_para_processador(df_consolidado)
            
            # 4.1. Sinistros COM deduções
            print("      - Filtrando sinistros com deduções...")
            df_sinistros_com_deducoes = ProcessadorDemonstracoes.filtrar_sinistros_com_deducoes(df_normalizado)
            df_sinistros_com_deducoes = ProcessadorDemonstracoes.remover_valores_zero(df_sinistros_com_deducoes)
            df_sinistros_formatado = ProcessadorDemonstracoes.preparar_csv_sinistros_com_deducoes(df_sinistros_com_deducoes)
            
            # 4.2. Sinistros SEM deduções (agregado)
            print("      - Filtrando sinistros sem deduções...")
            df_sinistros_sem_deducoes = ProcessadorDemonstracoes.filtrar_sinistros_sem_deducoes(df_normalizado)
            df_sinistros_sem_deducoes = ProcessadorDemonstracoes.remover_valores_zero(df_sinistros_sem_deducoes)
            
            print("      - Agregando sinistros...")
            colunas_agrupamento = ['reg_ans', 'cnpj', 'razao_social_operadora', 'trimestre', 'ano']
            df_sinistros_agregado = ProcessadorDemonstracoes.agregar_sinistros_sem_deducoes(
                df_sinistros_sem_deducoes, 
                colunas_agrupamento
            )
            df_sinistros_sem_deducoes_formatado = ProcessadorDemonstracoes.preparar_csv_sinistros_sem_deducoes(df_sinistros_agregado)
            
            # 5. Salvar arquivos CSV
            arquivo_com_deducoes = os.path.join(diretorio_destino, 'consolidado_despesas_sinistros_c_deducoes.csv')
            arquivo_sem_deducoes = os.path.join(diretorio_destino, 'sinistro_sem_deducoes.csv')
            
            # Formatar valores numéricos para formato brasileiro antes de salvar
            df_sinistros_formatado_br = self._formatar_valores_brasileiros(df_sinistros_formatado)
            df_sinistros_sem_deducoes_formatado_br = self._formatar_valores_brasileiros(df_sinistros_sem_deducoes_formatado)
            
            df_sinistros_formatado_br.to_csv(arquivo_com_deducoes, sep=';', index=False, encoding='utf-8-sig')
            print(f"      ✓ {os.path.basename(arquivo_com_deducoes)} ({len(df_sinistros_formatado):,} registros)")
            
            df_sinistros_sem_deducoes_formatado_br.to_csv(arquivo_sem_deducoes, sep=';', index=False, encoding='utf-8-sig')
            print(f"      ✓ {os.path.basename(arquivo_sem_deducoes)} ({len(df_sinistros_sem_deducoes_formatado):,} registros)")
            
            # 6. Gerar ZIP com os 2 arquivos + log
            print("\n    Gerando arquivo ZIP...")
            arquivo_zip = os.path.join(diretorio_destino, 'consolidado_despesas.zip')
            
            with zipfile.ZipFile(arquivo_zip, 'w', zipfile.ZIP_DEFLATED) as zipf:
                zipf.write(arquivo_com_deducoes, os.path.basename(arquivo_com_deducoes))
                zipf.write(arquivo_sem_deducoes, os.path.basename(arquivo_sem_deducoes))
                
                # Adicionar log se existir
                if arquivo_log and os.path.exists(arquivo_log):
                    zipf.write(arquivo_log, os.path.basename(arquivo_log))
            
            print(f"    ✓ {os.path.basename(arquivo_zip)}")
            
            # 7. Remover CSVs individuais (manter apenas ZIP)
            try:
                os.remove(arquivo_com_deducoes)
                os.remove(arquivo_sem_deducoes)
            except Exception as e:
                logger.warning(f"Falha ao remover CSVs temporários: {e}")
            
            return {
                "sucesso": True,
                "total_registros": total,
                "com_operadora": com_operadora,
                "sem_operadora": sem_operadora,
                "arquivos_gerados": [arquivo_zip],
                "registros_com_deducoes": len(df_sinistros_formatado),
                "registros_sem_deducoes": len(df_sinistros_sem_deducoes_formatado)
            }
        
        except Exception as e:
            logger.error(f"Erro ao gerar consolidados: {e}")
            return {
                "sucesso": False,
                "erro": str(e),
                "total_registros": 0,
                "com_operadora": 0,
                "sem_operadora": 0,
                "arquivos_gerados": []
            }
    
    def _carregar_operadoras_dataframe(self, diretorio: str) -> pd.DataFrame:
        """Carrega operadoras ativas e canceladas dos CSVs, priorizando ativas.
        
        Args:
            diretorio: Diretório raiz com arquivos extraídos
            
        Returns:
            DataFrame com operadoras consolidadas (sem duplicatas)
        """
        # Buscar em múltiplos locais possíveis
        caminhos_possiveis = [
            os.path.join(diretorio, "operadoras"),
            os.path.join(diretorio, "downloads", "operadoras"),
            diretorio
        ]
        
        ativas_path = None
        canceladas_path = None
        
        for base_dir in caminhos_possiveis:
            teste_ativas = os.path.join(base_dir, "operadoras_ativas.csv")
            teste_canceladas = os.path.join(base_dir, "operadoras_canceladas.csv")
            
            if os.path.exists(teste_ativas):
                ativas_path = teste_ativas
            if os.path.exists(teste_canceladas):
                canceladas_path = teste_canceladas
            
            if ativas_path and canceladas_path:
                break
        
        if not ativas_path and not canceladas_path:
            logger.warning("Nenhum arquivo de operadoras encontrado")
            return None
        
        dfs = []
        
        # Carregar ativas
        if ativas_path and os.path.exists(ativas_path):
            try:
                ativas = pd.read_csv(ativas_path, sep=';', encoding='utf-8-sig')
                # Normalizar nomes de colunas
                ativas.columns = ativas.columns.str.lower().str.strip()
                ativas['status'] = 'ATIVA'
                dfs.append(ativas)
                logger.info(f"✓ {len(ativas)} operadoras ativas carregadas")
            except Exception as e:
                logger.error(f"Erro ao carregar operadoras ativas: {e}")
        
        # Carregar canceladas
        if canceladas_path and os.path.exists(canceladas_path):
            try:
                canceladas = pd.read_csv(canceladas_path, sep=';', encoding='utf-8-sig')
                # Normalizar nomes de colunas
                canceladas.columns = canceladas.columns.str.lower().str.strip()
                canceladas['status'] = 'CANCELADA'
                dfs.append(canceladas)
                logger.info(f"✓ {len(canceladas)} operadoras canceladas carregadas")
            except Exception as e:
                logger.error(f"Erro ao carregar operadoras canceladas: {e}")
        
        if not dfs:
            return None
        
        # Concatenar e consolidar
        operadoras = pd.concat(dfs, ignore_index=True)
        
        # Converter REG_ANS para Int64 (nullable integer) para match correto
        operadoras['reg_ans'] = pd.to_numeric(
            operadoras['reg_ans'], 
            errors='coerce'
        ).astype('Int64')
        
        # Remover duplicatas, priorizando ativas (sort coloca ATIVA antes de CANCELADA)
        operadoras = operadoras.sort_values('status')
        operadoras = operadoras.drop_duplicates(subset=['reg_ans'], keep='first')
        
        logger.info(f"✓ Total de {len(operadoras)} operadoras únicas (após consolidação)")
        
        return operadoras
    
    def _listar_csvs_extraidos(self, diretorio: str) -> list:
        """Lista todos os CSVs extraídos dos ZIPs.
        
        Args:
            diretorio: Diretório base
            
        Returns:
            Lista de caminhos completos dos CSVs encontrados
        """
        caminhos_possiveis = [
            os.path.join(diretorio, "arquivos_trimestres", "extracted"),
            os.path.join(diretorio, "extracted"),
            os.path.join(diretorio, "trimestre_extraido"),
        ]
        
        for base_dir in caminhos_possiveis:
            if os.path.exists(base_dir):
                csvs = [
                    os.path.join(base_dir, f)
                    for f in os.listdir(base_dir)
                    if f.endswith('.csv')
                ]
                if csvs:
                    logger.info(f"✓ Encontrados {len(csvs)} CSVs em {base_dir}")
                    return csvs
        
        return []
    
    def _carregar_despesas_do_caminho(self, caminho: str) -> pd.DataFrame:
        """Carrega CSV de despesas de um caminho específico.
        
        Args:
            caminho: Caminho completo do arquivo CSV
            
        Returns:
            DataFrame com despesas ou None se erro
        """
        try:
            df = pd.read_csv(caminho, sep=';', encoding='utf-8-sig')
            
            # Normalizar nomes de colunas
            df.columns = df.columns.str.upper().str.strip()
            
            # Extrair TRIMESTRE e ANO do nome do arquivo (ex: 1T2025.csv)
            nome_arquivo = os.path.basename(caminho)
            import re
            match = re.match(r'(\d)T(\d{4})\.csv', nome_arquivo)
            
            if match:
                df['TRIMESTRE'] = f"{match.group(1)}T"
                df['ANO'] = int(match.group(2))
                logger.debug(f"Extraído do nome do arquivo: {match.group(1)}T/{match.group(2)}")
            else:
                # Fallback: tentar extrair da coluna DATA
                logger.debug(f"Tentando extrair TRIMESTRE/ANO da coluna DATA...")
                if 'DATA' in df.columns:
                    try:
                        # Converter DATA para datetime
                        df['DATA_TEMP'] = pd.to_datetime(df['DATA'], format='%d/%m/%Y', errors='coerce')
                        
                        # Extrair mês e calcular trimestre
                        df['MES'] = df['DATA_TEMP'].dt.month
                        df['TRIMESTRE'] = df['MES'].apply(lambda mes: f"{(mes - 1) // 3 + 1}T" if pd.notnull(mes) else None)
                        df['ANO'] = df['DATA_TEMP'].dt.year
                        
                        # Remover colunas temporárias
                        df = df.drop(columns=['DATA_TEMP', 'MES'])
                        
                        # Verificar se conseguiu extrair
                        if df['TRIMESTRE'].notna().any():
                            logger.debug(f"Extraído da coluna DATA: TRIMESTRE e ANO")
                        else:
                            logger.warning(f"Não foi possível extrair TRIMESTRE/ANO do arquivo {nome_arquivo}")
                    
                    except Exception as e:
                        logger.warning(f"Erro ao extrair TRIMESTRE/ANO da coluna DATA: {e}")
                        logger.warning(f"Não foi possível extrair TRIMESTRE/ANO do arquivo {nome_arquivo}")
                else:
                    logger.warning(f"Coluna DATA não encontrada. Não foi possível extrair TRIMESTRE/ANO do arquivo {nome_arquivo}")
            
            # Converter REG_ANS (ou REGISTROANS) para Int64 para match correto no JOIN
            coluna_reg = None
            if 'REGISTROANS' in df.columns:
                coluna_reg = 'REGISTROANS'
            elif 'REG_ANS' in df.columns:
                coluna_reg = 'REG_ANS'
            
            if coluna_reg:
                df[coluna_reg] = pd.to_numeric(
                    df[coluna_reg],
                    errors='coerce'
                ).astype('Int64')
            else:
                logger.warning(f"Coluna de registro ANS não encontrada em {os.path.basename(caminho)}")
                logger.warning(f"Colunas disponíveis: {list(df.columns)}")
            
            logger.info(f"✓ {len(df)} registros carregados de {os.path.basename(caminho)}")
            return df
        
        except Exception as e:
            logger.error(f"Erro ao carregar {caminho}: {e}")
            return None
    
    def _carregar_despesas(self, diretorio: str, nome_arquivo: str) -> pd.DataFrame:
        """Carrega arquivo de despesas/sinistros dos CSVs extraídos.
        
        Args:
            diretorio: Diretório base
            nome_arquivo: Nome do arquivo CSV a buscar
            
        Returns:
            DataFrame com despesas ou None se não encontrado
        """
        # Buscar em múltiplos locais possíveis
        caminhos_possiveis = [
            os.path.join(diretorio, "arquivos_trimestres", "extracted", nome_arquivo),
            os.path.join(diretorio, "extracted", nome_arquivo),
            os.path.join(diretorio, "trimestre_extraido", nome_arquivo),
            os.path.join(diretorio, nome_arquivo),
        ]
        
        for caminho in caminhos_possiveis:
            if os.path.exists(caminho):
                try:
                    df = pd.read_csv(caminho, sep=';', encoding='utf-8-sig')
                    
                    # Normalizar nomes de colunas
                    df.columns = df.columns.str.upper().str.strip()
                    
                    # Converter REGISTROANS para Int64 para match correto no JOIN
                    if 'REGISTROANS' in df.columns:
                        df['REGISTROANS'] = pd.to_numeric(
                            df['REGISTROANS'],
                            errors='coerce'
                        ).astype('Int64')
                    
                    logger.info(f"✓ {len(df)} registros carregados de {nome_arquivo}")
                    return df
                
                except Exception as e:
                    logger.error(f"Erro ao carregar {caminho}: {e}")
                    continue
        
        logger.warning(f"Arquivo {nome_arquivo} não encontrado em nenhum local")
        return None
    
    def _fazer_join(
        self,
        despesas_df: pd.DataFrame,
        operadoras_df: pd.DataFrame
    ) -> pd.DataFrame:
        """Faz JOIN entre despesas e operadoras.
        
        Args:
            despesas_df: DataFrame com despesas
            operadoras_df: DataFrame com operadoras
            
        Returns:
            DataFrame com resultado do JOIN
        """
        # Identificar coluna de registro ANS
        coluna_despesas = 'REG_ANS' if 'REG_ANS' in despesas_df.columns else 'REGISTROANS'
        
        # JOIN usando pandas merge (pegando CNPJ, razao_social, modalidade, uf)
        resultado = pd.merge(
            despesas_df,
            operadoras_df[['reg_ans', 'cnpj', 'razao_social', 'modalidade', 'uf']],
            left_on=coluna_despesas,
            right_on='reg_ans',
            how='left'
        )
        
        # Renomear colunas para formato esperado
        resultado = resultado.rename(columns={
            'cnpj': 'CNPJ',
            'razao_social': 'RAZAO_SOCIAL',
            'modalidade': 'MODALIDADE',
            'uf': 'UF'
        })
        
        # Preencher valores ausentes com N/L
        resultado['CNPJ'] = resultado['CNPJ'].fillna('N/L')
        resultado['RAZAO_SOCIAL'] = resultado['RAZAO_SOCIAL'].fillna('N/L')
        resultado['MODALIDADE'] = resultado['MODALIDADE'].fillna('N/L')
        resultado['UF'] = resultado['UF'].fillna('N/L')
        
        # Remover coluna reg_ans duplicada
        if 'reg_ans' in resultado.columns:
            resultado = resultado.drop(columns=['reg_ans'])
        
        return resultado
    
    def _normalizar_colunas_para_processador(self, df: pd.DataFrame) -> pd.DataFrame:
        """Normaliza colunas do DataFrame para o formato esperado pelo ProcessadorDemonstracoes.
        
        Args:
            df: DataFrame consolidado
            
        Returns:
            DataFrame com colunas normalizadas
        """
        # Mapear colunas para o formato esperado
        mapeamento = {
            'REGISTROANS': 'reg_ans',
            'REG_ANS': 'reg_ans',
            'RAZAO_SOCIAL': 'razao_social_operadora',
            'CNPJ': 'cnpj',
            'CD_CONTA_CONTABIL': 'cd_conta_contabil',
            'DESCRICAO': 'descricao',
            'VL_SALDO_INICIAL': 'vl_saldo_inicial',
            'VL_SALDO_FINAL': 'vl_saldo_final',
            'TRIMESTRE': 'trimestre',
            'ANO': 'ano'
        }
        
        # Renomear colunas que existem
        colunas_renomear = {k: v for k, v in mapeamento.items() if k in df.columns}
        df = df.rename(columns=colunas_renomear)
        
        # Converter valores numéricos APENAS se forem strings
        for col in ['vl_saldo_inicial', 'vl_saldo_final']:
            if col in df.columns:
                # Se for objeto (string), fazer conversão
                if df[col].dtype == 'object':
                    # Remover separadores de milhares e trocar vírgula por ponto
                    df[col] = df[col].astype(str).str.replace('.', '', regex=False).str.replace(',', '.', regex=False)
                    df[col] = pd.to_numeric(df[col], errors='coerce')
                # Se já for numérico, não fazer nada
        
        # Calcular valor_trimestre se não existir
        if 'valor_trimestre' not in df.columns:
            if 'vl_saldo_inicial' in df.columns and 'vl_saldo_final' in df.columns:
                df['valor_trimestre'] = df['vl_saldo_final'] - df['vl_saldo_inicial']
        
        return df
    
    def _formatar_valores_brasileiros(self, df: pd.DataFrame) -> pd.DataFrame:
        """Formata APENAS a coluna de valores para o padrão brasileiro (vírgula decimal).
        
        Args:
            df: DataFrame com valores numéricos
            
        Returns:
            DataFrame com coluna VALOR DE DESPESAS formatada
        """
        df_formatado = df.copy()
        
        # Formatar APENAS a coluna de valores (não REGISTRO ANS, CONTA CONTÁBIL, etc.)
        if 'VALOR DE DESPESAS' in df_formatado.columns:
            df_formatado['VALOR DE DESPESAS'] = df_formatado['VALOR DE DESPESAS'].apply(
                lambda x: f"{x:,.2f}".replace(',', 'X').replace('.', ',').replace('X', '.') if pd.notnull(x) else ''
            )
        
        return df_formatado
    
    def _fazer_join_e_salvar(
        self,
        despesas_df: pd.DataFrame,
        operadoras_df: pd.DataFrame,
        diretorio_destino: str,
        nome_arquivo: str
    ) -> Dict:
        """Faz JOIN entre despesas e operadoras e salva resultado.
        
        Args:
            despesas_df: DataFrame com despesas
            operadoras_df: DataFrame com operadoras
            diretorio_destino: Diretório para salvar
            nome_arquivo: Nome do arquivo de saída
            
        Returns:
            Dict com estatísticas do JOIN
        """
        # Identificar coluna de registro ANS
        coluna_despesas = 'REG_ANS' if 'REG_ANS' in despesas_df.columns else 'REGISTROANS'
        
        # JOIN usando pandas merge
        resultado = pd.merge(
            despesas_df,
            operadoras_df[['reg_ans', 'razao_social', 'modalidade', 'uf']],
            left_on=coluna_despesas,
            right_on='reg_ans',
            how='left'
        )
        
        # Renomear colunas para formato esperado
        resultado = resultado.rename(columns={
            'razao_social': 'RAZAO_SOCIAL',
            'modalidade': 'MODALIDADE',
            'uf': 'UF'
        })
        
        # Preencher valores ausentes com N/L
        resultado['RAZAO_SOCIAL'] = resultado['RAZAO_SOCIAL'].fillna('N/L')
        resultado['MODALIDADE'] = resultado['MODALIDADE'].fillna('N/L')
        resultado['UF'] = resultado['UF'].fillna('N/L')
        
        # Remover coluna reg_ans (duplicada)
        if 'reg_ans' in resultado.columns:
            resultado = resultado.drop(columns=['reg_ans'])
        
        # Calcular estatísticas
        total = len(resultado)
        com_operadora = (resultado['RAZAO_SOCIAL'] != 'N/L').sum()
        sem_operadora = total - com_operadora
        
        # Salvar
        caminho_saida = os.path.join(diretorio_destino, nome_arquivo)
        
        # Garantir que o diretório existe e tem permissões
        os.makedirs(diretorio_destino, exist_ok=True)
        
        resultado.to_csv(caminho_saida, sep=';', index=False, encoding='utf-8-sig')
        
        logger.info(
            f"Consolidado gerado: {nome_arquivo} "
            f"(total={total}, com_operadora={com_operadora}, sem_operadora={sem_operadora})"
        )
        
        return {
            "arquivo": caminho_saida,
            "total_registros": total,
            "com_operadora": com_operadora,
            "sem_operadora": sem_operadora
        }
