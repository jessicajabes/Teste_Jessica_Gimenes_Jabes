import os
import pandas as pd
from typing import List, Dict
from config import DIRETORIO_ERROS
from infraestrutura.logger import get_logger

logger = get_logger('RepositorioBancoDados')

class RepositorioBancoDados:
    def __init__(self, url_conexao: str):
        self.url_conexao = url_conexao
        self.conexao = None
    
    def conectar(self) -> bool:
        try:
            import psycopg2
            self.conexao = psycopg2.connect(self.url_conexao)
            return True
        except Exception as e:
            print(f"Erro ao conectar no banco: {e}")
            print(f"DATABASE_URL usada: {self.url_conexao}")
            return False
    
    def desconectar(self):
        if self.conexao:
            self.conexao.close()
    
    def inserir_demonstracoes(self, dados: List[Dict], arquivo_origem: str = None) -> int:
        if not self.conexao:
            return 0
        
        try:
            cursor = self.conexao.cursor()
            processados = 0
            erros = 0
            erros_detalhados = {}
            registros_com_erro = []
            
            for idx, registro in enumerate(dados):
                idx_real = idx  # Número real da linha no arquivo original
                contexto_base = (
                    f"arquivo={arquivo_origem or 'desconhecido'}, "
                    f"linha={idx_real + 1}, "
                    f"reg_ans={registro.get('REG_ANS')}, "
                    f"cd_conta={registro.get('CD_CONTA_CONTABIL')}"
                )
                # Validar se há campos vazios (CNPJ/REG_ANS ou DESCRICAO/Razão Social)
                validacao = self._validar_campos_obrigatorios(registro)
                
                if validacao['tem_erro']:
                    # Inserir com campos NULL e registrar o erro
                    erros += 1
                    
                    sql = """
                        INSERT INTO demonstracoes_contabeis_temp 
                        (data, reg_ans, cd_conta_contabil, descricao, vl_saldo_inicial, vl_saldo_final, valor_trimestre, trimestre, ano)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (reg_ans, cd_conta_contabil, trimestre, ano) DO NOTHING
                    """
                    
                    try:
                        # Se REG_ANS está vazio, usar NULL
                        reg_ans = self._limpar_valor(registro.get('REG_ANS'))
                        reg_ans = reg_ans if reg_ans else None
                        
                        # Se DESCRICAO está vazio, usar NULL
                        descricao = self._limpar_valor(registro.get('DESCRICAO'))
                        descricao = descricao if descricao else None
                        
                        vl_saldo_inicial = self._normalizar_numero(
                            registro.get('VL_SALDO_INICIAL'),
                            campo='VL_SALDO_INICIAL',
                            contexto=contexto_base
                        )
                        vl_saldo_final = self._normalizar_numero(
                            registro.get('VL_SALDO_FINAL'),
                            campo='VL_SALDO_FINAL',
                            contexto=contexto_base
                        )
                        
                        # Calcular valor do trimestre
                        valor_trimestre = None
                        if vl_saldo_inicial is not None and vl_saldo_final is not None:
                            valor_trimestre = vl_saldo_final - vl_saldo_inicial
                        
                        valores = (
                            registro.get('DATA'),
                            reg_ans,
                            registro.get('CD_CONTA_CONTABIL'),
                            descricao,
                            vl_saldo_inicial,
                            vl_saldo_final,
                            valor_trimestre,
                            registro.get('TRIMESTRE'),
                            registro.get('ANO')
                        )
                        
                        cursor.execute(sql, valores)
                        processados += 1
                        
                        # Registrar aviso no arquivo de erros
                        from datetime import datetime
                        registros_com_erro.append({
                            'arquivo_origem': arquivo_origem or 'desconhecido',
                            'linha_arquivo': idx_real + 1,
                            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                            'reg_ans': registro.get('REG_ANS'),
                            'cd_conta_contabil': registro.get('CD_CONTA_CONTABIL'),
                            'descricao': registro.get('DESCRICAO'),
                            'vl_saldo_inicial': registro.get('VL_SALDO_INICIAL'),
                            'vl_saldo_final': registro.get('VL_SALDO_FINAL'),
                            'trimestre': registro.get('TRIMESTRE'),
                            'ano': registro.get('ANO'),
                            'motivo_erro': validacao['mensagem'],
                            'tipo_erro': 'AVISO_VALIDACAO',
                            'origem': 'Validação de campos obrigatórios'
                        })
                        logger.warning(f"Aviso validação em {arquivo_origem}:{idx_real + 1} - {validacao['mensagem']}")
                        
                        if erros <= 3:
                            print(f"      ⚠ Aviso no registro {idx}: {validacao['mensagem']}")
                            print(f"        Dados inseridos com NULL nos campos vazios")
                        
                    except Exception as e:
                        erro_msg = str(e)
                        from datetime import datetime
                        registros_com_erro.append({
                            'arquivo_origem': arquivo_origem or 'desconhecido',
                            'linha_arquivo': idx_real + 1,
                            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                            'reg_ans': registro.get('REG_ANS'),
                            'cd_conta_contabil': registro.get('CD_CONTA_CONTABIL'),
                            'descricao': registro.get('DESCRICAO'),
                            'vl_saldo_inicial': registro.get('VL_SALDO_INICIAL'),
                            'vl_saldo_final': registro.get('VL_SALDO_FINAL'),
                            'trimestre': registro.get('TRIMESTRE'),
                            'ano': registro.get('ANO'),
                            'motivo_erro': f"{validacao['mensagem']} + Erro ao inserir: {erro_msg}",
                            'tipo_erro': 'VALIDACAO+INSERCAO',
                            'origem': 'Validação + Inserção no banco de dados'
                        })
                        self.conexao.rollback()
                        cursor = self.conexao.cursor()
                else:
                    # Inserir normalmente
                    sql = """
                        INSERT INTO demonstracoes_contabeis_temp 
                        (data, reg_ans, cd_conta_contabil, descricao, vl_saldo_inicial, vl_saldo_final, valor_trimestre, trimestre, ano)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (reg_ans, cd_conta_contabil, trimestre, ano) DO NOTHING
                    """
                    
                    try:
                        vl_saldo_inicial = self._normalizar_numero(
                            registro.get('VL_SALDO_INICIAL'),
                            campo='VL_SALDO_INICIAL',
                            contexto=contexto_base
                        )
                        vl_saldo_final = self._normalizar_numero(
                            registro.get('VL_SALDO_FINAL'),
                            campo='VL_SALDO_FINAL',
                            contexto=contexto_base
                        )
                        
                        # Calcular valor do trimestre
                        valor_trimestre = None
                        if vl_saldo_inicial is not None and vl_saldo_final is not None:
                            valor_trimestre = vl_saldo_final - vl_saldo_inicial
                        
                        valores = (
                            registro.get('DATA'),
                            registro.get('REG_ANS'),
                            registro.get('CD_CONTA_CONTABIL'),
                            registro.get('DESCRICAO'),
                            vl_saldo_inicial,
                            vl_saldo_final,
                            valor_trimestre,
                            registro.get('TRIMESTRE'),
                            registro.get('ANO')
                        )
                        
                        cursor.execute(sql, valores)
                        processados += 1
                    except Exception as e:
                        erros += 1
                        erro_msg = str(e)
                        from datetime import datetime
                        
                        logger.error(f"Erro ao inserir em {arquivo_origem}:{idx_real + 1} - {erro_msg}")
                        
                        if erro_msg not in erros_detalhados:
                            erros_detalhados[erro_msg] = {
                                'count': 0,
                                'amostra': registro
                            }
                        erros_detalhados[erro_msg]['count'] += 1
                        
                        # Adicionar registro com erro para gerar CSV
                        registros_com_erro.append({
                            'arquivo_origem': arquivo_origem or 'desconhecido',
                            'linha_arquivo': idx_real + 1,
                            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                            'reg_ans': registro.get('REG_ANS'),
                            'cd_conta_contabil': registro.get('CD_CONTA_CONTABIL'),
                            'descricao': registro.get('DESCRICAO'),
                            'vl_saldo_inicial': registro.get('VL_SALDO_INICIAL'),
                            'vl_saldo_final': registro.get('VL_SALDO_FINAL'),
                            'trimestre': registro.get('TRIMESTRE'),
                            'ano': registro.get('ANO'),
                            'motivo_erro': erro_msg,
                            'tipo_erro': 'INSERCAO_BANCO',
                            'origem': 'Inserção no banco de dados'
                        })
                        
                        if erros <= 3:
                            print(f"      Erro ao inserir registro {idx}: {e}")
                            print(f"      Valores: reg_ans={registro.get('REG_ANS')}, cd_conta={registro.get('CD_CONTA_CONTABIL')}, trimestre={registro.get('TRIMESTRE')}, ano={registro.get('ANO')}")
                        self.conexao.rollback()
                        cursor = self.conexao.cursor()
            
            self.conexao.commit()
            cursor.close()
            
            # Exibir resumo de erros
            if registros_com_erro:
                print(f"\n      === RESUMO DE AVISOS E ERROS ({len(registros_com_erro)} total) ===")
                
                # Agrupar por tipo de erro
                erros_por_tipo = {}
                for reg_erro in registros_com_erro:
                    motivo = reg_erro['motivo_erro']
                    if motivo not in erros_por_tipo:
                        erros_por_tipo[motivo] = 0
                    erros_por_tipo[motivo] += 1
                
                for motivo, count in erros_por_tipo.items():
                    print(f"      - {count}x: {motivo}")
                
                # Gerar CSV de erros
                self._gerar_csv_erros(registros_com_erro)
            
            return processados
        except Exception as e:
            if self.conexao:
                self.conexao.rollback()
            print(f"      Erro geral na inserção: {e}")
            return 0

    @staticmethod
    def _normalizar_numero(valor, campo: str = None, contexto: str = None):
        if valor is None:
            return None
        try:
            if isinstance(valor, str):
                valor_limpo = valor.strip()
                valor_transformado = valor_limpo.replace('.', '').replace(',', '.')
                numero = float(valor_transformado)
                return numero
            return float(valor)
        except (ValueError, TypeError) as e:
            # Log apenas quando houver erro na conversão
            detalhes = f"campo={campo}" if campo else "campo=desconhecido"
            if contexto:
                detalhes = f"{detalhes}, {contexto}"
            logger.warning(f"Erro na normalização numérica ({detalhes}): valor='{valor}' - {str(e)}")
            return None
    
    @staticmethod
    def _limpar_valor(valor):
        """Remove espaços em branco e retorna None se vazio"""
        if valor is None:
            return None
        
        valor_str = str(valor).strip()
        return valor_str if valor_str else None
    
    @staticmethod
    def _validar_campos_obrigatorios(registro: Dict) -> Dict:
        """Valida se os campos obrigatórios estão preenchidos
        
        Retorna dict com:
        - tem_erro: bool indicando se há erro
        - mensagem: str descrevendo os campos vazios
        """
        campos_vazios = []
        
        reg_ans = registro.get('REG_ANS')
        if not reg_ans or not str(reg_ans).strip():
            campos_vazios.append('CNPJ/REG_ANS')
        
        descricao = registro.get('DESCRICAO')
        if not descricao or not str(descricao).strip():
            campos_vazios.append('Razão Social/DESCRICAO')
        
        if campos_vazios:
            return {
                'tem_erro': True,
                'mensagem': f"Campos vazios: {', '.join(campos_vazios)}"
            }
        
        return {
            'tem_erro': False,
            'mensagem': ''
        }
    
    def _gerar_csv_erros(self, registros_com_erro: List[Dict]):
        try:
            import pandas as pd
            from datetime import datetime
            
            df_erros = pd.DataFrame(registros_com_erro)
            
            # Ordenar por timestamp e linha
            if 'timestamp' in df_erros.columns:
                df_erros = df_erros.sort_values(['timestamp', 'linha'], ascending=[False, True])
            
            os.makedirs(DIRETORIO_ERROS, exist_ok=True)
            caminho_erros = os.path.join(DIRETORIO_ERROS, 'erros_insercao.csv')
            df_erros.to_csv(caminho_erros, index=False, encoding='utf-8', sep=';')
            
            # Criar também um resumo de erros
            caminho_resumo = os.path.join(DIRETORIO_ERROS, 'erros_resumo.txt')
            with open(caminho_resumo, 'w', encoding='utf-8') as f:
                f.write(f"RESUMO DE ERROS - Gerado em {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
                f.write("="*80 + "\n\n")
                f.write(f"Total de erros: {len(registros_com_erro)}\n\n")
                
                # Agrupar por tipo de erro
                if 'motivo_erro' in df_erros.columns:
                    erros_agrupados = df_erros.groupby('motivo_erro').size().sort_values(ascending=False)
                    f.write("Erros por motivo:\n")
                    f.write("-" * 80 + "\n")
                    for motivo, count in erros_agrupados.items():
                        f.write(f"  {count:>6}x - {motivo}\n")
                
                # Agrupar por tipo
                if 'tipo_erro' in df_erros.columns:
                    f.write("\n" + "="*80 + "\n")
                    f.write("Erros por tipo:\n")
                    f.write("-" * 80 + "\n")
                    tipos_agrupados = df_erros.groupby('tipo_erro').size()
                    for tipo, count in tipos_agrupados.items():
                        f.write(f"  {count:>6}x - {tipo}\n")
            
            print(f"\n      [OK] Arquivos de erros gerados:")
            print(f"         [DETALHADO] {caminho_erros}")
            print(f"         [RESUMO] {caminho_resumo}")
        except Exception as e:
            print(f"      [ERRO] Erro ao gerar CSV de erros: {e}")

    def registrar_erro_validacao(self, info: Dict):
        try:
            os.makedirs(DIRETORIO_ERROS, exist_ok=True)
            caminho_erros = os.path.join(DIRETORIO_ERROS, 'erros_insercao.csv')
            df_novo = pd.DataFrame([info])

            if os.path.exists(caminho_erros):
                try:
                    df_existente = pd.read_csv(caminho_erros)
                    df_final = pd.concat([df_existente, df_novo], ignore_index=True)
                except Exception:
                    df_final = df_novo
            else:
                df_final = df_novo

            df_final.to_csv(caminho_erros, index=False, encoding='utf-8')
            print(f"      Aviso registrado no arquivo de erros: {caminho_erros}")
        except Exception as e:
            print(f"      Erro ao registrar aviso no CSV de erros: {e}")
    
    def limpar_tabela(self) -> bool:
        if not self.conexao:
            return False
        
        try:
            cursor = self.conexao.cursor()
            cursor.execute("DELETE FROM demonstracoes_contabeis_temp")
            self.conexao.commit()
            cursor.close()
            return True
        except Exception as e:
            if self.conexao:
                self.conexao.rollback()
            return False

    def limpar_tabela_operadoras(self) -> bool:
        """Limpa tabela de operadoras"""
        if not self.conexao:
            return False
        
        try:
            cursor = self.conexao.cursor()
            cursor.execute("DELETE FROM operadoras")
            self.conexao.commit()
            cursor.close()
            logger.info("Tabela operadoras limpa com sucesso")
            return True
        except Exception as e:
            logger.error(f"Erro ao limpar tabela operadoras: {e}")
            if self.conexao:
                self.conexao.rollback()
            return False

    def inserir_operadoras(self, dados: List[Dict]) -> int:
        """Insere operadoras no banco de dados em lote"""
        if not self.conexao:
            return 0
        
        if not dados:
            return 0
        
        try:
            cursor = self.conexao.cursor()
            inseridos = 0
            erros = 0
            
            sql = """
                INSERT INTO operadoras 
                (reg_ans, cnpj, razao_social, modalidade, uf, status)
                VALUES (%s, %s, %s, %s, %s, %s)
            """
            
            for registro in dados:
                try:
                    reg_ans_raw = registro.get('REG_ANS')
                    if pd.isna(reg_ans_raw) or not str(reg_ans_raw).strip() or str(reg_ans_raw).strip().lower() == 'nan':
                        erros += 1
                        continue

                    valores = (
                        str(reg_ans_raw).strip(),
                        '' if pd.isna(registro.get('CNPJ')) else str(registro.get('CNPJ', '')).strip(),
                        '' if pd.isna(registro.get('RAZAO_SOCIAL')) else str(registro.get('RAZAO_SOCIAL', '')).strip(),
                        '' if pd.isna(registro.get('MODALIDADE')) else str(registro.get('MODALIDADE', '')).strip(),
                        '' if pd.isna(registro.get('UF')) else str(registro.get('UF', '')).strip().upper(),
                        str(registro.get('STATUS', 'DESCONHECIDA')).strip().upper()
                    )
                    cursor.execute(sql, valores)
                    # Contar como inserido se execute foi bem-sucedido
                    inseridos += 1
                except Exception as e_inner:
                    erros += 1
                    logger.debug(f"Erro ao inserir operadora {registro.get('REG_ANS')}: {str(e_inner)}")
                    # Continuar com próximo registro
                    self.conexao.rollback()
                    cursor = self.conexao.cursor()
            
            self.conexao.commit()
            cursor.close()
            logger.info(f"{inseridos} operadoras inseridas no banco (com {erros} erros)")
            return inseridos
            
        except Exception as e:
            logger.error(f"Erro ao inserir operadoras: {e}")
            if self.conexao:
                self.conexao.rollback()
            return 0

    def gerar_csv_consolidado_com_join(self, diretorio_saida: str, arquivo_log_sessao: str = None) -> bool:
        """Gera CSVs consolidados fazendo JOIN com operadoras no banco"""
        if not self.conexao:
            return False
        
        try:
            import os
            from sqlalchemy import create_engine
            os.makedirs(diretorio_saida, exist_ok=True)
            
            # Converter URL de conexão PostgreSQL para SQLAlchemy
            # De: postgresql://user:password@host:port/dbname
            # Para: postgresql+psycopg2://user:password@host:port/dbname
            url_sqlalchemy = self.url_conexao
            if url_sqlalchemy.startswith('postgresql://'):
                url_sqlalchemy = url_sqlalchemy.replace('postgresql://', 'postgresql+psycopg2://', 1)
            
            # Criar engine SQLAlchemy para read_sql_query
            try:
                engine = create_engine(url_sqlalchemy, echo=False)
            except Exception as e_engine:
                logger.debug(f"Erro ao criar engine com {url_sqlalchemy}: {e_engine}")
                # Tentar com URL original
                engine = create_engine(self.url_conexao)
            
            # Query com LEFT JOIN para enriquecer com dados de operadoras
            # (sem duplicar linhas quando houver múltiplas operadoras por reg_ans)
            query = """
                WITH operadoras_agregadas AS (
                    SELECT
                        reg_ans,
                        COUNT(*) AS qtd_operadoras,
                        MAX(cnpj) AS cnpj,
                        MAX(razao_social) AS razao_social,
                        MAX(modalidade) AS modalidade,
                        MAX(uf) AS uf,
                        MAX(status) AS status
                    FROM operadoras
                    GROUP BY reg_ans
                )
                SELECT 
                    d.data,
                    d.reg_ans,
                    d.cd_conta_contabil,
                    d.descricao,
                    d.vl_saldo_inicial,
                    d.vl_saldo_final,
                    d.valor_trimestre,
                    d.trimestre,
                    d.ano,
                    CASE
                        WHEN o.reg_ans IS NULL THEN 'N/L'
                        WHEN o.qtd_operadoras > 1 THEN 'DUPLICIDADE'
                        ELSE COALESCE(o.cnpj, 'N/L')
                    END as cnpj,
                    CASE
                        WHEN o.reg_ans IS NULL THEN 'N/L'
                        WHEN o.qtd_operadoras > 1 THEN 'DUPLICIDADE'
                        ELSE COALESCE(o.razao_social, 'N/L')
                    END as razao_social_operadora,
                    COALESCE(o.modalidade, 'N/L') as modalidade,
                    COALESCE(o.uf, 'N/L') as uf,
                    CASE 
                        WHEN o.reg_ans IS NULL THEN 'NAO_LOCALIZADO'
                        WHEN o.qtd_operadoras > 1 THEN 'DUPLICIDADE'
                        ELSE o.status
                    END as status_operadora
                FROM demonstracoes_contabeis_temp d
                LEFT JOIN operadoras_agregadas o ON d.reg_ans = o.reg_ans
                ORDER BY d.ano, d.trimestre, d.reg_ans, d.cd_conta_contabil
            """
            
            # Ler dados do banco usando SQLAlchemy
            df = pd.read_sql_query(query, engine)
            
            if df.empty:
                logger.warning("Nenhum dado encontrado para gerar CSV")
                return False

            # Registrar erros quando não há match ou há duplicidade na operadora
            try:
                from datetime import datetime
                erros_join = []
                mascara_erro = df['razao_social_operadora'].isin(['N/L', 'DUPLICIDADE'])
                if mascara_erro.any():
                    df_erros = df.loc[mascara_erro]
                    for _, linha in df_erros.iterrows():
                        motivo = 'OPERADORA_NAO_LOCALIZADA' if linha['razao_social_operadora'] == 'N/L' else 'OPERADORA_DUPLICADA'
                        erros_join.append({
                            'arquivo_origem': 'JOIN_CONSOLIDADO',
                            'linha_arquivo': None,
                            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                            'reg_ans': linha.get('reg_ans'),
                            'cd_conta_contabil': linha.get('cd_conta_contabil'),
                            'descricao': linha.get('descricao'),
                            'vl_saldo_inicial': linha.get('vl_saldo_inicial'),
                            'vl_saldo_final': linha.get('vl_saldo_final'),
                            'trimestre': linha.get('trimestre'),
                            'ano': linha.get('ano'),
                            'motivo_erro': motivo,
                            'tipo_erro': 'JOIN_OPERADORA',
                            'origem': 'Consolidação via JOIN'
                        })
                    if erros_join:
                        self._gerar_csv_erros(erros_join)
                        logger.warning(f"JOIN com {len(erros_join)} registros com operadora N/L ou DUPLICIDADE")
            except Exception as e:
                logger.error(f"Erro ao registrar problemas de JOIN: {e}")
            
            # Filtrar despesas com sinistros ANTES de normalizar
            df_sinistros = df[df['descricao'].str.contains('Despesas com Eventos/Sinistros', case=False, na=False)]
            
            # Função para normalizar valores numéricos para formato brasileiro (vírgula)
            def normalizar_para_br(df_temp):
                """Converte valores numéricos para formato brasileiro (vírgula como decimal)"""
                df_copy = df_temp.copy()
                colunas_numericas = ['vl_saldo_inicial', 'vl_saldo_final', 'valor_trimestre']
                
                for col in colunas_numericas:
                    if col in df_copy.columns:
                        # Converter para string com 2 casas decimais e substituir ponto por vírgula
                        df_copy[col] = df_copy[col].apply(
                            lambda x: f"{float(x):,.2f}".replace(',', '#').replace('.', ',').replace('#', '.') 
                            if pd.notna(x) else ''
                        )
                
                return df_copy
            
            # Salvar cópia com valores numéricos para cálculo posterior
            df_numerico = df.copy()
            df_sinistros_numerico = df_sinistros.copy()
            
            # Normalizar DataFrames para formato brasileiro
            df_sinistros_br = normalizar_para_br(df_sinistros)
            df_br = normalizar_para_br(df)
            
            # Gerar CSV de despesas com sinistros
            arquivo_sinistros = os.path.join(diretorio_saida, 'consolidado_despesas_sinistros.csv')
            # Garantir UTF-8 com BOM para compatibilidade com Excel
            df_sinistros_br.to_csv(arquivo_sinistros, index=False, encoding='utf-8-sig', sep=';')
            logger.info(f"CSV despesas sinistros gerado: {len(df_sinistros)} registros")
            print(f"  [CSV] {arquivo_sinistros} ({len(df_sinistros)} registros)")
            
            # Gerar CSV com todas as despesas
            arquivo_todas = os.path.join(diretorio_saida, 'consolidado_todas_despesas.csv')
            # Garantir UTF-8 com BOM para compatibilidade com Excel
            df_br.to_csv(arquivo_todas, index=False, encoding='utf-8-sig', sep=';')
            logger.info(f"CSV todas despesas gerado: {len(df)} registros")
            
            # Salvar arquivo com valores numéricos originais para cálculos internos
            arquivo_numerico = os.path.join(diretorio_saida, '.consolidado_todas_despesas_numerico.csv')
            df_numerico.to_csv(arquivo_numerico, index=False, encoding='utf-8-sig', sep=';')
            logger.debug(f"CSV numérico (interno) salvo para cálculos: {arquivo_numerico}")
            print(f"  [CSV] {arquivo_todas} ({len(df)} registros)")
            
            # Gerar ZIP
            import zipfile
            arquivo_zip = os.path.join(diretorio_saida, 'consolidado_despesas.zip')
            with zipfile.ZipFile(arquivo_zip, 'w', zipfile.ZIP_DEFLATED) as zipf:
                zipf.write(arquivo_sinistros, os.path.basename(arquivo_sinistros))
                zipf.write(arquivo_todas, os.path.basename(arquivo_todas))
                if arquivo_log_sessao and os.path.exists(arquivo_log_sessao):
                    zipf.write(arquivo_log_sessao, os.path.basename(arquivo_log_sessao))
            logger.info(f"ZIP consolidado gerado: {arquivo_zip}")
            print(f"  [ZIP] {arquivo_zip}")
            
            return True
            
        except Exception as e:
            logger.error(f"Erro ao gerar CSV consolidado: {e}")
            print(f"  [ERRO] Erro ao gerar CSV: {e}")
            return False

    def calcular_valor_total_csv(self, diretorio_saida: str) -> float:
        """Calcula o valor total (VL_SALDO_FINAL - VL_SALDO_INICIAL) do CSV consolidado gerado"""
        try:
            import os
            import re
            
            # Usar o arquivo formatado (com valores em formato brasileiro)
            arquivo_csv = os.path.join(diretorio_saida, 'consolidado_todas_despesas.csv')
            
            if not os.path.exists(arquivo_csv):
                logger.warning(f"Arquivo CSV não encontrado: {arquivo_csv}")
                return 0.0
            
            # Ler o CSV gerado com encoding correto
            df = pd.read_csv(arquivo_csv, sep=';', encoding='utf-8-sig')
            
            # Procurar pela coluna valor_trimestre (já é a diferença calculada)
            coluna_valor = None
            for col in df.columns:
                if 'valor_trimestre' in str(col).lower():
                    coluna_valor = col
                    break
            
            if coluna_valor is None:
                # Se não encontrar valor_trimestre, calcular a diferença
                coluna_final = None
                coluna_inicial = None
                
                for col in df.columns:
                    if 'vl_saldo_final' in str(col).lower():
                        coluna_final = col
                    elif 'vl_saldo_inicial' in str(col).lower():
                        coluna_inicial = col
                
                if coluna_final is None or coluna_inicial is None:
                    logger.warning("Colunas de valores não encontradas no CSV")
                    return 0.0
                
                # Converter para numérico, tratando formato brasileiro (1.234,56 -> 1234.56)
                def br_to_float(val):
                    if pd.isna(val) or val == '':
                        return 0.0
                    val_str = str(val).strip()
                    # Remover espaços, pontos (milhares) e substituir vírgula por ponto
                    val_str = val_str.replace(' ', '').replace('.', '').replace(',', '.')
                    try:
                        return float(val_str)
                    except:
                        return 0.0
                
                df[coluna_final] = df[coluna_final].apply(br_to_float)
                df[coluna_inicial] = df[coluna_inicial].apply(br_to_float)
                
                # Calcular diferença (VL_SALDO_FINAL - VL_SALDO_INICIAL)
                valor_total = (df[coluna_final] - df[coluna_inicial]).sum()
            else:
                # Usar valor_trimestre diretamente (já é a diferença)
                def br_to_float(val):
                    if pd.isna(val) or val == '':
                        return 0.0
                    val_str = str(val).strip()
                    # Remover espaços, pontos (milhares) e substituir vírgula por ponto
                    val_str = val_str.replace(' ', '').replace('.', '').replace(',', '.')
                    try:
                        return float(val_str)
                    except:
                        return 0.0
                
                df[coluna_valor] = df[coluna_valor].apply(br_to_float)
                valor_total = df[coluna_valor].sum()
            
            logger.info(f"Valor total final calculado do CSV (FINAL - INICIAL): {valor_total}")
            return valor_total
            
        except Exception as e:
            logger.error(f"Erro ao calcular valor total do CSV: {e}")
            return 0.0
