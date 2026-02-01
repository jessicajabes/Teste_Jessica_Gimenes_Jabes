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
            from datetime import datetime
            cursor = self.conexao.cursor()
            processados = 0
            erros = 0
            registros_com_erro = []
            
            # Separar registros válidos e inválidos
            registros_validos = []
            registros_invalidos = []
            
            for idx, registro in enumerate(dados):
                validacao = self._validar_campos_obrigatorios(registro)
                
                if validacao['tem_erro']:
                    registros_invalidos.append((idx, registro, validacao))
                else:
                    registros_validos.append((idx, registro))
            
            # ========== PROCESSAR REGISTROS VÁLIDOS EM BATCH ==========
            if registros_validos:
                valores_batch = []
                
                for idx, registro in registros_validos:
                    contexto_base = (
                        f"arquivo={arquivo_origem or 'desconhecido'}, "
                        f"linha={idx + 1}, "
                        f"reg_ans={registro.get('REG_ANS')}, "
                        f"cd_conta={registro.get('CD_CONTA_CONTABIL')}"
                    )
                    
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
                    
                    valor_trimestre = None
                    if vl_saldo_inicial is not None and vl_saldo_final is not None:
                        valor_trimestre = vl_saldo_final - vl_saldo_inicial
                    
                    valores_batch.append((
                        registro.get('DATA'),
                        registro.get('REG_ANS'),
                        registro.get('CD_CONTA_CONTABIL'),
                        registro.get('DESCRICAO'),
                        vl_saldo_inicial,
                        vl_saldo_final,
                        valor_trimestre,
                        registro.get('TRIMESTRE'),
                        registro.get('ANO')
                    ))
                
                # Inserir todos de uma vez
                if valores_batch:
                    sql = """
                        INSERT INTO demonstracoes_contabeis_temp 
                        (data, reg_ans, cd_conta_contabil, descricao, vl_saldo_inicial, vl_saldo_final, valor_trimestre, trimestre, ano)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (reg_ans, cd_conta_contabil, trimestre, ano) DO NOTHING
                    """
                    try:
                        cursor.executemany(sql, valores_batch)
                        processados = len(valores_batch)
                        self.conexao.commit()
#                        logger.debug(f"[BATCH] {processados} registros inseridos com sucesso")
                    except Exception as e:
                        self.conexao.rollback()
                        cursor = self.conexao.cursor()
                        logger.error(f"Erro ao inserir batch em {arquivo_origem}: {e}")
                        erros += len(valores_batch)
            
            # ========== PROCESSAR REGISTROS INVÁLIDOS ==========
            if registros_invalidos:
                for idx, registro, validacao in registros_invalidos:
                    erros += 1
                    
                    sql = """
                        INSERT INTO demonstracoes_contabeis_temp 
                        (data, reg_ans, cd_conta_contabil, descricao, vl_saldo_inicial, vl_saldo_final, valor_trimestre, trimestre, ano)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (reg_ans, cd_conta_contabil, trimestre, ano) DO NOTHING
                    """
                    
                    try:
                        reg_ans = self._limpar_valor(registro.get('REG_ANS')) or None
                        descricao = self._limpar_valor(registro.get('DESCRICAO')) or None
                        
                        contexto_base = (
                            f"arquivo={arquivo_origem or 'desconhecido'}, "
                            f"linha={idx + 1}"
                        )
                        
                        vl_saldo_inicial = self._normalizar_numero(
                            registro.get('VL_SALDO_INICIAL'),
                            contexto=contexto_base
                        )
                        vl_saldo_final = self._normalizar_numero(
                            registro.get('VL_SALDO_FINAL'),
                            contexto=contexto_base
                        )
                        
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
                        
                   #     cursor.execute(sql, valores)
                #    self.conexao.commit()
                        processados += 1
                        
                        registros_com_erro.append({
                            'arquivo_origem': arquivo_origem or 'desconhecido',
                            'linha_arquivo': idx + 1,
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
                        
                        logger.warning(f"Aviso em {arquivo_origem}:{idx + 1} - {validacao['mensagem']}")
                        
                    except Exception as e:
                        self.conexao.rollback()
                        cursor = self.conexao.cursor()
                        logger.error(f"Erro ao inserir registro com aviso {arquivo_origem}:{idx + 1} - {e}")
                        registros_com_erro.append({
                            'arquivo_origem': arquivo_origem or 'desconhecido',
                            'linha_arquivo': idx + 1,
                            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                            'reg_ans': registro.get('REG_ANS'),
                            'cd_conta_contabil': registro.get('CD_CONTA_CONTABIL'),
                            'descricao': registro.get('DESCRICAO'),
                            'vl_saldo_inicial': registro.get('VL_SALDO_INICIAL'),
                            'vl_saldo_final': registro.get('VL_SALDO_FINAL'),
                            'trimestre': registro.get('TRIMESTRE'),
                            'ano': registro.get('ANO'),
                            'motivo_erro': str(e),
                            'tipo_erro': 'VALIDACAO+INSERCAO',
                            'origem': 'Validação + Inserção'
                        })
            
            cursor.close()
            
            # Gerar CSV de erros se houver
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
            
            # Garantir que o diretório existe e tem permissões
            os.makedirs(diretorio_saida, mode=0o777, exist_ok=True)
            
            # Tentar dar permissão total à pasta
            try:
                os.chmod(diretorio_saida, 0o777)
            except:
                pass  # Ignorar erros de chmod
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
            # Se houver duplicidade, verifica quantas têm status ATIVO
            # Se apenas uma for ATIVO, usa ela; senão marca como DUPLICIDADE
            query = """
                WITH operadoras_agregadas AS (
                    SELECT
                        reg_ans,
                        COUNT(*) AS qtd_operadoras,
                        COUNT(*) FILTER (WHERE UPPER(status) = 'ATIVO') AS qtd_ativas,
                        MAX(CASE WHEN UPPER(status) = 'ATIVO' THEN cnpj ELSE NULL END) AS cnpj_ativo,
                        MAX(cnpj) AS cnpj,
                        MAX(CASE WHEN UPPER(status) = 'ATIVO' THEN razao_social ELSE NULL END) AS razao_social_ativa,
                        MAX(razao_social) AS razao_social,
                        MAX(CASE WHEN UPPER(status) = 'ATIVO' THEN modalidade ELSE NULL END) AS modalidade_ativa,
                        MAX(modalidade) AS modalidade,
                        MAX(CASE WHEN UPPER(status) = 'ATIVO' THEN uf ELSE NULL END) AS uf_ativo,
                        MAX(uf) AS uf,
                        MAX(CASE WHEN UPPER(status) = 'ATIVO' THEN status ELSE NULL END) AS status_ativo,
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
                        WHEN o.qtd_operadoras > 1 AND o.qtd_ativas = 1 THEN COALESCE(o.cnpj_ativo, 'N/L')
                        WHEN o.qtd_operadoras > 1 THEN 'REGISTRO DE OPERADORA EM DUPLICIDADE'
                        ELSE COALESCE(o.cnpj, 'N/L')
                    END as cnpj,
                    CASE
                        WHEN o.reg_ans IS NULL THEN 'N/L'
                        WHEN o.qtd_operadoras > 1 AND o.qtd_ativas = 1 THEN COALESCE(o.razao_social_ativa, 'N/L')
                        WHEN o.qtd_operadoras > 1 THEN 'REGISTRO DE OPERADORA EM DUPLICIDADE'
                        ELSE COALESCE(o.razao_social, 'N/L')
                    END as razao_social_operadora,
                    CASE
                        WHEN o.reg_ans IS NULL THEN 'N/L'
                        WHEN o.qtd_operadoras > 1 AND o.qtd_ativas = 1 THEN COALESCE(o.modalidade_ativa, 'N/L')
                        WHEN o.qtd_operadoras > 1 THEN 'N/L'
                        ELSE COALESCE(o.modalidade, 'N/L')
                    END as modalidade,
                    CASE
                        WHEN o.reg_ans IS NULL THEN 'N/L'
                        WHEN o.qtd_operadoras > 1 AND o.qtd_ativas = 1 THEN COALESCE(o.uf_ativo, 'N/L')
                        WHEN o.qtd_operadoras > 1 THEN 'N/L'
                        ELSE COALESCE(o.uf, 'N/L')
                    END as uf,
                    CASE 
                        WHEN o.reg_ans IS NULL THEN 'NAO_LOCALIZADO'
                        WHEN o.qtd_operadoras > 1 AND o.qtd_ativas = 1 THEN COALESCE(o.status_ativo, 'ATIVO')
                        WHEN o.qtd_operadoras > 1 THEN 'REGISTRO DE OPERADORA EM DUPLICIDADE'
                        ELSE COALESCE(o.status, 'DESCONHECIDO')
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
                mascara_erro = df['razao_social_operadora'].isin(['N/L', 'REGISTRO DE OPERADORA EM DUPLICIDADE'])
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
            # Inclui a linha principal ("Despesas com Eventos/Sinistros")
            # + todas as linhas de deduções logo abaixo (começando com "-" ou "(-)")
            # Para quando encontrar uma que não atenda aos critérios
            indices_sinistros = set()
            
            for idx, row in df.iterrows():
                # Adicionar linha principal de sinistros
                # Aceita variações: "Despesas com Eventos/Sinistros" ou "Despesas com Eventos / Sinistros"
                if isinstance(row['descricao'], str) and 'Despesas com Eventos' in row['descricao'] and 'Sinistros' in row['descricao']:
                    indices_sinistros.add(idx)
                    
                    # Procurar próximas linhas (sem limite) que começam com "-" ou "(-)"
                    for idx_prox in range(idx + 1, len(df)):
                        row_prox = df.iloc[idx_prox]
                        
                        # Verificar se começa com "-" ou "(-)"
                        descricao_prox = str(row_prox['descricao']).strip()
                        if not (descricao_prox.startswith('-') or descricao_prox.startswith('(-)')):
                            break

                        # Exigir cd_conta_contabil com 9 dígitos
                        cd_conta_str = str(row_prox['cd_conta_contabil']).strip()
                        if len(cd_conta_str) == 9:
                            indices_sinistros.add(idx_prox)
                        else:
                            break
            
            df_sinistros = df.loc[list(indices_sinistros)] if indices_sinistros else df.iloc[0:0]
            
            # Filtrar sinistros SEM deduções
            # Apenas linhas que contêm "Despesas com Eventos" E "Sinistros"
            # E que têm cd_conta com 9 dígitos começando com "4"
            indices_sinistros_sem_deducoes = set()
            
            for idx, row in df.iterrows():
                if isinstance(row['descricao'], str) and 'Despesas com Eventos' in row['descricao'] and 'Sinistros' in row['descricao']:
                    cd_conta_str = str(row['cd_conta_contabil']).strip()
                    if len(cd_conta_str) == 9 and cd_conta_str.startswith('4'):
                        indices_sinistros_sem_deducoes.add(idx)
            
            df_sinistros_sem_deducoes = df.loc[list(indices_sinistros_sem_deducoes)] if indices_sinistros_sem_deducoes else df.iloc[0:0]

            # Remover registros com valor de despesas igual a 0
            if not df_sinistros.empty:
                df_sinistros = df_sinistros[df_sinistros['valor_trimestre'].fillna(0) != 0]
            if not df_sinistros_sem_deducoes.empty:
                df_sinistros_sem_deducoes = df_sinistros_sem_deducoes[df_sinistros_sem_deducoes['valor_trimestre'].fillna(0) != 0]
            
            # Filtrar apenas despesas (cd_conta_contabil começa com '4')
            df_despesas = df[df['cd_conta_contabil'].astype(str).str.startswith('4', na=False)]

            # Garantir a mesma ordenação do SQL em todos os arquivos gerados
            colunas_ordem = ['ano', 'trimestre', 'reg_ans', 'cd_conta_contabil']
            if not df_sinistros.empty:
                df_sinistros = df_sinistros.sort_values(colunas_ordem, kind='mergesort')
            if not df_sinistros_sem_deducoes.empty:
                df_sinistros_sem_deducoes = df_sinistros_sem_deducoes.sort_values(colunas_ordem, kind='mergesort')
            if not df_despesas.empty:
                df_despesas = df_despesas.sort_values(colunas_ordem, kind='mergesort')
            if not df.empty:
                df = df.sort_values(colunas_ordem, kind='mergesort')
            
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
            # df_numerico = df.copy()
            # df_sinistros_numerico = df_sinistros.copy()
            # df_sinistros_sem_deducoes_numerico = df_sinistros_sem_deducoes.copy()
            # df_despesas_numerico = df_despesas.copy()
            
            # Normalizar DataFrames para formato brasileiro
            df_sinistros_br = normalizar_para_br(df_sinistros)
            df_sinistros_sem_deducoes_br = normalizar_para_br(df_sinistros_sem_deducoes)
            df_br = normalizar_para_br(df)
            df_despesas_br = normalizar_para_br(df_despesas)

            # Gerar CSVs em diretório temporário (serão adicionados ao ZIP apenas)
            temp_dir = os.path.join(diretorio_saida, '_tmp_consolidados')
            os.makedirs(temp_dir, exist_ok=True)

            # Gerar CSV de despesas com sinistros (colunas específicas e ordem definida)
            arquivo_sinistros = os.path.join(temp_dir, 'consolidado_despesas_sinistros_c_deducoes.csv')
            df_sinistros_saida = df_sinistros_br[[
                'cnpj',
                'razao_social_operadora',
                'trimestre',
                'ano',
                'valor_trimestre',
                'reg_ans',
                'cd_conta_contabil',
                'descricao'
            ]].rename(columns={
                'cnpj': 'CNPJ',
                'razao_social_operadora': 'RAZAOSOCIAL',
                'trimestre': 'TRIMESTRE',
                'ano': 'ANO',
                'valor_trimestre': 'VALOR DE DESPESAS',
                'reg_ans': 'REGISTRO ANS',
                'cd_conta_contabil': 'CONTA CONTÁBIL',
                'descricao':'DESCRICAO'
            })
            # Garantir UTF-8 com BOM para compatibilidade com Excel
            df_sinistros_saida.to_csv(arquivo_sinistros, index=False, encoding='utf-8-sig', sep=';')
            logger.info(f"CSV despesas sinistros gerado: {len(df_sinistros)} registros")
            print(f"  [CSV] {os.path.basename(arquivo_sinistros)} ({len(df_sinistros)} registros)")
            
            # Gerar CSV de sinistros SEM deduções
            arquivo_sinistros_sem_deducoes = os.path.join(temp_dir, 'sinistro_sem_deducoes.csv')
            # Agrupar por REG_ANS, CNPJ, RAZAO_SOCIAL, TRIMESTRE, ANO e somar VALOR_TRIMESTRE
            # IMPORTANTE: Fazer a agregação ANTES de normalizar para formato brasileiro
            df_sinistros_sem_deducoes_numerico = df_sinistros_sem_deducoes.copy()
            df_sinistros_sem_deducoes_agrupado = df_sinistros_sem_deducoes_numerico.groupby(
                ['reg_ans', 'cnpj', 'razao_social_operadora', 'trimestre', 'ano'],
                as_index=False
            ).agg({
                'valor_trimestre': 'sum'
            })
            
            # Agora normalizar para formato brasileiro
            df_sinistros_sem_deducoes_agrupado_br = normalizar_para_br(df_sinistros_sem_deducoes_agrupado)
            
            df_sinistros_sem_deducoes_saida = df_sinistros_sem_deducoes_agrupado_br[[
                'reg_ans',
                'cnpj',
                'razao_social_operadora',
                'trimestre',
                'ano',
                'valor_trimestre'
            ]].rename(columns={
                'reg_ans':'REG. ANS',
                'cnpj': 'CNPJ',
                'razao_social_operadora': 'RAZAOSOCIAL',
                'trimestre': 'TRIMESTRE',
                'ano': 'ANO',
                'valor_trimestre': 'VALOR DE DESPESAS'
            })
            
            # Garantir ordenação
            df_sinistros_sem_deducoes_saida = df_sinistros_sem_deducoes_saida.sort_values(
                ['ANO', 'TRIMESTRE', 'REG. ANS', 'CNPJ'],
                kind='mergesort'
            )

            # Garantir UTF-8 com BOM para compatibilidade com Excel
            df_sinistros_sem_deducoes_saida.to_csv(arquivo_sinistros_sem_deducoes, index=False, encoding='utf-8-sig', sep=';')
            logger.info(f"CSV sinistros sem deduções gerado: {len(df_sinistros_sem_deducoes_agrupado)} registros agregados (de {len(df_sinistros_sem_deducoes)} originais)")
            print(f"  [CSV] {os.path.basename(arquivo_sinistros_sem_deducoes)} ({len(df_sinistros_sem_deducoes_agrupado)} registros agregados)")
            
            # Gerar CSV com apenas despesas (cd_conta_contabil começa com '4')
            arquivo_despesas = os.path.join(temp_dir, 'demonstracoes_despesas.csv')
            # Garantir UTF-8 com BOM para compatibilidade com Excel
            df_despesas_br.to_csv(arquivo_despesas, index=False, encoding='utf-8-sig', sep=';')
            logger.info(f"CSV despesas gerado: {len(df_despesas)} registros")
            print(f"  [CSV] {os.path.basename(arquivo_despesas)} ({len(df_despesas)} registros)")
            
            # Gerar CSV com todas as demonstrações
            arquivo_todas = os.path.join(temp_dir, 'demonstracoes_contabeis_completo.csv')
            # Garantir UTF-8 com BOM para compatibilidade com Excel
            df_br.to_csv(arquivo_todas, index=False, encoding='utf-8-sig', sep=';')
            logger.info(f"CSV todas demonstracoes gerado: {len(df)} registros")
            
            # Gerar ZIP
            import zipfile
            arquivo_zip = os.path.join(diretorio_saida, 'consolidado_despesas.zip')
            with zipfile.ZipFile(arquivo_zip, 'w', zipfile.ZIP_DEFLATED) as zipf:
                zipf.write(arquivo_sinistros, os.path.basename(arquivo_sinistros))
                zipf.write(arquivo_sinistros_sem_deducoes, os.path.basename(arquivo_sinistros_sem_deducoes))
                zipf.write(arquivo_despesas, os.path.basename(arquivo_despesas))
                zipf.write(arquivo_todas, os.path.basename(arquivo_todas))
                if arquivo_log_sessao and os.path.exists(arquivo_log_sessao):
                    zipf.write(arquivo_log_sessao, os.path.basename(arquivo_log_sessao))
            logger.info(f"ZIP consolidado gerado: {arquivo_zip}")
            print(f"  [ZIP] {arquivo_zip}")

            # Remover arquivos temporários (mantém apenas o ZIP)
            try:
                for caminho in [
                    arquivo_sinistros,
                    arquivo_sinistros_sem_deducoes,
                    arquivo_despesas,
                    arquivo_todas,
                ]:
                    if os.path.exists(caminho):
                        os.remove(caminho)
                if os.path.isdir(temp_dir) and not os.listdir(temp_dir):
                    os.rmdir(temp_dir)
            except Exception as e_cleanup:
                logger.warning(f"Falha ao limpar arquivos temporários: {e_cleanup}")
            
            return True
            
        except Exception as e:
            logger.error(f"Erro ao gerar CSV consolidado: {e}")
            print(f"  [ERRO] Erro ao gerar CSV: {e}")
            return False

    def calcular_valor_total_csv(self, diretorio_saida: str) -> float:
        """Calcula o valor total direto do banco de dados (soma de valor_trimestre)"""
        try:
            if not self.conexao:
                logger.warning("Sem conexão com banco para calcular valor total")
                return 0.0
            
            cursor = self.conexao.cursor()
            
            # Calcular soma direto do banco (muito mais rápido e confiável)
            sql = """
                SELECT 
                    COALESCE(SUM(valor_trimestre), 0) as total_trimestre,
                    COALESCE(SUM(vl_saldo_final - vl_saldo_inicial), 0) as total_diferenca,
                    COUNT(*) as total_registros
                FROM demonstracoes_contabeis_temp
            """
            
            cursor.execute(sql)
            resultado = cursor.fetchone()
            cursor.close()
            
            if resultado:
                total_trimestre = float(resultado[0] or 0)
                total_diferenca = float(resultado[1] or 0)
                total_registros = int(resultado[2] or 0)
                
                logger.info(f"Cálculo do banco: {total_registros} registros")
                logger.info(f"  - Soma(valor_trimestre): {total_trimestre:,.2f}")
                logger.info(f"  - Soma(final - inicial): {total_diferenca:,.2f}")
                
                # Usar valor_trimestre (mais confiável pois foi calculado na inserção)
                return total_trimestre
            
            return 0.0
            
        except Exception as e:
            logger.error(f"Erro ao calcular valor total do banco: {e}")
            return 0.0
