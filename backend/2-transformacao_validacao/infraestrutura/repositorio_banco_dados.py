"""
Repositório de banco de dados para transformação e validação
"""

import pandas as pd
import os
from typing import List, Dict
from domain.repositorios import RepositorioBanco
from config import DIRETORIO_CONSOLIDADOS, DIRETORIO_TRANSFORMACAO, DIRETORIO_INTEGRACAO

class RepositorioBancoDados(RepositorioBanco):
    def __init__(self, url_conexao: str, diretorio_dados: str = DIRETORIO_CONSOLIDADOS):
        self.url_conexao = url_conexao
        self.diretorio_dados = diretorio_dados
        self.diretorio_saida = DIRETORIO_TRANSFORMACAO
        self.arquivo_erros = os.path.join(self.diretorio_saida, "erros_insercao.csv")
        self.arquivo_erros_entrada = os.path.join(DIRETORIO_INTEGRACAO, "erros", "erros_insercao.csv")
        self.conexao = None
        
        # Criar diretório de saída se não existir
        os.makedirs(self.diretorio_saida, exist_ok=True)
    
    def conectar(self) -> bool:
        try:
            import psycopg2
            self.conexao = psycopg2.connect(self.url_conexao)
            print("✓ Conexão com banco de dados estabelecida")
            return True
        except Exception as e:
            print(f"✗ Erro ao conectar no banco: {e}")
            return False
    
    def desconectar(self):
        if self.conexao:
            self.conexao.close()
            print("✓ Desconectado do banco de dados")
    
    def inserir_demonstracoes(self, dados: List[Dict]) -> int:
        """Insere demonstrações contábeis no banco"""
        if not self.conexao:
            return 0
        
        try:
            cursor = self.conexao.cursor()
            inseridos = 0
            erros = 0
            registros_com_erro = []
            
            for idx, registro in enumerate(dados):
                # Validar se há campos vazios (CNPJ/REG_ANS ou DESCRICAO/Razão Social)
                validacao = self._validar_campos_obrigatorios(registro)
                
                if validacao['tem_erro']:
                    # Inserir com campos NULL e registrar o aviso
                    erros += 1
                    
                    sql = """
                        INSERT INTO demonstracoes_contabeis_temp 
                        (data, reg_ans, cd_conta_contabil, descricao, vl_saldo_inicial, vl_saldo_final, trimestre, ano)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (reg_ans, cd_conta_contabil, trimestre, ano) DO NOTHING
                    """
                    
                    try:
                        # Se REG_ANS está vazio, usar NULL
                        reg_ans = self._limpar_valor(registro.get('REG_ANS'))
                        reg_ans = reg_ans if reg_ans else None
                        
                        # Se DESCRICAO está vazio, usar NULL
                        descricao = self._limpar_valor(registro.get('DESCRICAO'))
                        descricao = descricao if descricao else None
                        
                        valores = (
                            registro.get('DATA'),
                            reg_ans,
                            registro.get('CD_CONTA_CONTABIL'),
                            descricao,
                            self._normalizar_numero(registro.get('VL_SALDO_INICIAL')),
                            self._normalizar_numero(registro.get('VL_SALDO_FINAL')),
                            registro.get('TRIMESTRE'),
                            registro.get('ANO')
                        )
                        
                        cursor.execute(sql, valores)
                        inseridos += cursor.rowcount
                        
                        # Registrar aviso no arquivo de erros
                        registros_com_erro.append({
                            'reg_ans': registro.get('REG_ANS'),
                            'cd_conta_contabil': registro.get('CD_CONTA_CONTABIL'),
                            'descricao': registro.get('DESCRICAO'),
                            'vl_saldo_inicial': registro.get('VL_SALDO_INICIAL'),
                            'vl_saldo_final': registro.get('VL_SALDO_FINAL'),
                            'trimestre': registro.get('TRIMESTRE'),
                            'ano': registro.get('ANO'),
                            'motivo_erro': validacao['mensagem'],
                            'etapa': 'transformacao_banco_dados'
                        })
                        
                        if erros <= 3:
                            print(f"  ⚠ Aviso no registro {idx}: {validacao['mensagem']}")
                    
                    except Exception as e:
                        erro_msg = str(e)
                        registros_com_erro.append({
                            'reg_ans': registro.get('REG_ANS'),
                            'cd_conta_contabil': registro.get('CD_CONTA_CONTABIL'),
                            'descricao': registro.get('DESCRICAO'),
                            'vl_saldo_inicial': registro.get('VL_SALDO_INICIAL'),
                            'vl_saldo_final': registro.get('VL_SALDO_FINAL'),
                            'trimestre': registro.get('TRIMESTRE'),
                            'ano': registro.get('ANO'),
                            'motivo_erro': f"{validacao['mensagem']} + Erro ao inserir: {erro_msg}",
                            'etapa': 'transformacao_banco_dados'
                        })
                        self.conexao.rollback()
                        cursor = self.conexao.cursor()
                
                else:
                    # Inserir normalmente
                    sql = """
                        INSERT INTO demonstracoes_contabeis_temp 
                        (data, reg_ans, cd_conta_contabil, descricao, vl_saldo_inicial, vl_saldo_final, trimestre, ano)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (reg_ans, cd_conta_contabil, trimestre, ano) DO NOTHING
                    """
                    
                    try:
                        valores = (
                            registro.get('DATA'),
                            registro.get('REG_ANS'),
                            registro.get('CD_CONTA_CONTABIL'),
                            registro.get('DESCRICAO'),
                            self._normalizar_numero(registro.get('VL_SALDO_INICIAL')),
                            self._normalizar_numero(registro.get('VL_SALDO_FINAL')),
                            registro.get('TRIMESTRE'),
                            registro.get('ANO')
                        )
                        
                        cursor.execute(sql, valores)
                        inseridos += cursor.rowcount
                    except Exception as e:
                        erros += 1
                        erro_msg = str(e)
                        
                        # Adicionar registro com erro para gerar CSV
                        registros_com_erro.append({
                            'reg_ans': registro.get('REG_ANS'),
                            'cd_conta_contabil': registro.get('CD_CONTA_CONTABIL'),
                            'descricao': registro.get('DESCRICAO'),
                            'vl_saldo_inicial': registro.get('VL_SALDO_INICIAL'),
                            'vl_saldo_final': registro.get('VL_SALDO_FINAL'),
                            'trimestre': registro.get('TRIMESTRE'),
                            'ano': registro.get('ANO'),
                            'motivo_erro': erro_msg,
                            'etapa': 'transformacao_banco_dados'
                        })
                        
                        if erros <= 3:
                            print(f"  Erro ao inserir registro {idx}: {e}")
                        self.conexao.rollback()
                        cursor = self.conexao.cursor()
            
            self.conexao.commit()
            cursor.close()
            
            print(f"✓ {inseridos} registros inseridos com sucesso")
            if registros_com_erro:
                print(f"⚠ {len(registros_com_erro)} registros com aviso ou erro")
                # Gerar CSV de erros
                self._gerar_csv_erros(registros_com_erro)
            
            return inseridos
        except Exception as e:
            if self.conexao:
                self.conexao.rollback()
            print(f"✗ Erro geral na inserção: {e}")
            return 0
    
    def listar_demonstracoes(self, filtros: Dict = None) -> pd.DataFrame:
        """Lista demonstrações do banco com filtros opcionais"""
        try:
            from sqlalchemy import create_engine
            
            engine = create_engine(self.url_conexao)
            
            sql = "SELECT * FROM demonstracoes_contabeis_temp WHERE 1=1"
            
            if filtros:
                if 'ano' in filtros:
                    sql += f" AND ano = {filtros['ano']}"
                if 'trimestre' in filtros:
                    sql += f" AND trimestre = {filtros['trimestre']}"
                if 'reg_ans' in filtros:
                    sql += f" AND reg_ans = '{filtros['reg_ans']}'"
            
            sql += " ORDER BY ano DESC, trimestre DESC, reg_ans"
            
            df = pd.read_sql_query(sql, engine)
            engine.dispose()
            return df
        except Exception as e:
            print(f"✗ Erro ao listar demonstrações: {e}")
            return pd.DataFrame()
    
    def _gerar_csv_erros(self, registros_com_erro: List[Dict]):
        """Gera ou atualiza o arquivo CSV de erros"""
        try:
            df_novo = pd.DataFrame(registros_com_erro)
            
            # Verificar se já existe arquivo de erros da etapa anterior
            if os.path.exists(self.arquivo_erros_entrada):
                try:
                    df_existente = pd.read_csv(self.arquivo_erros_entrada)
                    # Concatenar com novos erros
                    df_final = pd.concat([df_existente, df_novo], ignore_index=True)
                    print(f"⚠ Acrescentando {len(df_novo)} novos erros ao arquivo existente da etapa anterior")
                except Exception:
                    # Se houver erro ao ler, criar novo
                    df_final = df_novo
            else:
                # Verificar se já existe arquivo na saída
                if os.path.exists(self.arquivo_erros):
                    try:
                        df_existente = pd.read_csv(self.arquivo_erros)
                        df_final = pd.concat([df_existente, df_novo], ignore_index=True)
                    except Exception:
                        df_final = df_novo
                else:
                    df_final = df_novo
            
            # Salvar arquivo de erros na pasta transformacao
            df_final.to_csv(self.arquivo_erros, index=False, encoding='utf-8')
            print(f"✓ Arquivo de erros atualizado: {self.arquivo_erros}")
        except Exception as e:
            print(f"✗ Erro ao gerar CSV de erros: {e}")
    
    @staticmethod
    def _normalizar_numero(valor):
        """Normaliza valor para float"""
        if valor is None:
            return None
        try:
            if isinstance(valor, str):
                valor_limpo = valor.strip().replace('.', '').replace(',', '.')
                return float(valor_limpo)
            return float(valor)
        except (ValueError, TypeError):
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
