"""
Importador de dados consolidados para o banco de dados
"""

import os
import time
import pandas as pd
from typing import List, Dict
from domain.entidades import DemonstracaoContabil, ResultadoImportacao
from infraestrutura.repositorio_csv import RepositorioCSVLocal
from infraestrutura.repositorio_banco_dados import RepositorioBancoDados
from config import DIRETORIO_CONSOLIDADOS, DIRETORIO_TRANSFORMACAO
from infraestrutura.logger import get_logger

logger = get_logger('ImportarDadosConsolidados')

class ImportarDadosConsolidados:
    """Importa dados dos CSVs consolidados para o banco de dados"""
    
    def __init__(self, url_banco: str, diretorio_dados: str = DIRETORIO_CONSOLIDADOS):
        self.url_banco = url_banco
        self.diretorio_dados = diretorio_dados
        self.diretorio_saida = DIRETORIO_TRANSFORMACAO
        self.repo_csv = RepositorioCSVLocal()
        self.repo_banco = RepositorioBancoDados(url_banco, diretorio_dados)
        self.arquivo_despesas_sinistros = os.path.join(diretorio_dados, "consolidado_despesas_sinistros.csv")
        self.arquivo_todas_despesas = os.path.join(diretorio_dados, "consolidado_todas_despesas.csv")
        self.arquivo_erros = os.path.join(self.diretorio_saida, "erros_insercao.csv")
    
    def executar(self) -> ResultadoImportacao:
        """Executa a importação completa"""
        inicio = time.time()
        
        print("="*60)
        print("IMPORTAÇÃO DE DADOS CONSOLIDADOS")
        print("="*60)
        
        # Verificar se há arquivo de erros anterior
        self._verificar_erros_anteriores()
        
        # Conectar ao banco
        if not self.repo_banco.conectar():
            return ResultadoImportacao(
                total_registros=0,
                registros_importados=0,
                registros_com_erro=0,
                erros=["Falha na conexão com banco de dados"],
                tempo_execucao=time.time() - inicio
            )
        
        try:
            # Carregar e importar despesas com sinistros
            print("\n1. Importando Despesas com Sinistros...")
            resultado_sinistros = self._importar_arquivo(
                self.arquivo_despesas_sinistros, 
                "Despesas com Sinistros"
            )
            
            # Carregar e importar todas as despesas
            print("\n2. Importando Todas as Despesas...")
            resultado_todas = self._importar_arquivo(
                self.arquivo_todas_despesas,
                "Todas as Despesas"
            )
            
            # Consolidar resultados
            tempo_execucao = time.time() - inicio
            
            resultado = ResultadoImportacao(
                total_registros=resultado_sinistros['total'] + resultado_todas['total'],
                registros_importados=resultado_sinistros['importados'] + resultado_todas['importados'],
                registros_com_erro=resultado_sinistros['erros'] + resultado_todas['erros'],
                erros=[],
                tempo_execucao=tempo_execucao
            )
            
            self._exibir_resumo(resultado)
            
            return resultado
        
        finally:
            self.repo_banco.desconectar()
    
    def _verificar_erros_anteriores(self):
        """Verifica se há arquivo de erros da etapa anterior"""
        if os.path.exists(self.arquivo_erros):
            try:
                df_erros = pd.read_csv(self.arquivo_erros)
                print(f"\n⚠ Arquivo de erros anterior encontrado: {len(df_erros)} registros")
                
                # Mostrar resumo dos erros por etapa
                if 'etapa' in df_erros.columns:
                    print("\nResumo de erros anteriores por etapa:")
                    for etapa, count in df_erros['etapa'].value_counts().items():
                        print(f"  - {etapa}: {count} erros")
            except Exception as e:
                print(f"⚠ Erro ao ler arquivo anterior: {e}")
    
    def _importar_arquivo(self, caminho: str, nome: str) -> Dict:
        """Importa um arquivo CSV específico"""
        resultado = {
            'total': 0,
            'importados': 0,
            'erros': 0
        }
        
        # Carregar CSV
        df = self.repo_csv.ler_arquivo(caminho)
        if df.empty:
            resultado['erros'] += 1
            return resultado
        
        resultado['total'] = len(df)
        
        # Converter para lista de dicionários
        registros = df.to_dict('records')
        
        # Inserir no banco
        importados = self.repo_banco.inserir_demonstracoes(registros)
        resultado['importados'] = importados
        resultado['erros'] = len(registros) - importados
        
        return resultado
    
    def _exibir_resumo(self, resultado: ResultadoImportacao):
        """Exibe resumo da importação"""
        print("\n" + "="*60)
        print("RESUMO DA IMPORTAÇÃO")
        print("="*60)
        print(f"Total de registros processados: {resultado.total_registros}")
        print(f"Registros importados com sucesso: {resultado.registros_importados}")
        print(f"Registros com erro: {resultado.registros_com_erro}")
        print(f"Taxa de sucesso: {(resultado.registros_importados / resultado.total_registros * 100):.2f}%" if resultado.total_registros > 0 else "0%")
        print(f"Tempo de execução: {resultado.tempo_execucao:.2f}s")
        
        # Verificar se há arquivo de erros final
        if os.path.exists(self.arquivo_erros):
            try:
                df_erros = pd.read_csv(self.arquivo_erros)
                print(f"\n📄 Arquivo de erros consolidado: {len(df_erros)} registros com erro")
            except:
                pass
        
        print("="*60 + "\n")
