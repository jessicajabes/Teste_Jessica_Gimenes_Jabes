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
