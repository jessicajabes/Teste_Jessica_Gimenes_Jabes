"""
Main - Transformação e Validação de Dados

Lê os arquivos CSV gerados pela integração com API e realiza
transformação, validação e importação dos dados no banco.
"""

import os
import logging
import pandas as pd
from typing import List, Dict
from config import DATABASE_URL, DIRETORIO_CONSOLIDADOS, DIRETORIO_TRANSFORMACAO

# Configurar diretório de logs ANTES de importar os outros módulos
log_dir = '/app/downloads/Integracao/logs'
os.makedirs(log_dir, exist_ok=True)

# Configurar logger global
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s | %(levelname)-8s | %(filename)s:%(lineno)d | %(funcName)s() | %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(log_dir, 'aplicacao.log'), encoding='utf-8'),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger('TransformacaoValidacao')

# Agora importar os outros módulos
from casos_uso.importar_dados_consolidados import ImportarDadosConsolidados
from casos_uso.gerar_despesas_agregadas import GerarDespesasAgregadas

logger.info("="*60)
logger.info("Iniciando Transformação e Validação de Dados")

class TransformacaoValidacao:
    def __init__(self, diretorio_dados: str = DIRETORIO_CONSOLIDADOS):
        self.diretorio_dados = diretorio_dados
        self.arquivo_despesas_sinistros = os.path.join(diretorio_dados, "consolidado_despesas_sinistros.csv")
        self.arquivo_todas_despesas = os.path.join(diretorio_dados, "consolidado_todas_despesas.csv")
    
    def executar(self):
        """Executa o processo de transformação, validação e importação"""
        print("="*60)
        print("TRANSFORMAÇÃO E VALIDAÇÃO DE DADOS")
        print("="*60)
        
        # Verificar se os arquivos existem
        if not self._verificar_arquivos():
            print("\nERRO: Arquivos CSV não encontrados!")
            print(f"Verifique se os arquivos estão em: {self.diretorio_dados}")
            return False
        
        # Carregar dados
        print("\n1. Carregando dados...")
        df_sinistros = self._carregar_csv(self.arquivo_despesas_sinistros, "Despesas com Sinistros")
        df_todas = self._carregar_csv(self.arquivo_todas_despesas, "Todas as Despesas")
        
        if df_sinistros is None or df_todas is None:
            return False
        
        # Realizar validações
        print("\n2. Realizando validações...")
        self._validar_dados(df_sinistros, "Despesas com Sinistros")
        self._validar_dados(df_todas, "Todas as Despesas")
        
        # Análise estatística
        print("\n3. Análise Estatística...")
        self._analise_estatistica(df_sinistros, "Despesas com Sinistros")
        self._analise_estatistica(df_todas, "Todas as Despesas")
        
        # Importar dados no banco
        print("\n4. Importando dados no banco de dados...")
        self._importar_dados()
        
        # Gerar despesas agregadas
        print("\n5. Gerando despesas agregadas...")
        self._gerar_despesas_agregadas()
        
        print("\n" + "="*60)
        print("TRANSFORMAÇÃO E VALIDAÇÃO CONCLUÍDA")
        print("="*60)
        
        # Flush dos logs
        for handler in logger.handlers:
            if isinstance(handler, logging.FileHandler):
                handler.flush()
        
        return True
    
    def _verificar_arquivos(self) -> bool:
        """Verifica se os arquivos CSV existem"""
        existe_sinistros = os.path.exists(self.arquivo_despesas_sinistros)
        existe_todas = os.path.exists(self.arquivo_todas_despesas)
        
        if existe_sinistros:
            print(f"✓ Encontrado: {os.path.basename(self.arquivo_despesas_sinistros)}")
        else:
            print(f"✗ Não encontrado: {os.path.basename(self.arquivo_despesas_sinistros)}")
        
        if existe_todas:
            print(f"✓ Encontrado: {os.path.basename(self.arquivo_todas_despesas)}")
        else:
            print(f"✗ Não encontrado: {os.path.basename(self.arquivo_todas_despesas)}")
        
        return existe_sinistros and existe_todas
    
    def _carregar_csv(self, caminho: str, nome: str) -> pd.DataFrame:
        """Carrega um arquivo CSV"""
        try:
            df = pd.read_csv(caminho, sep=';', encoding='utf-8')
            print(f"  {nome}: {len(df)} registros carregados")
            print(f"    Colunas: {len(df.columns)}")
            return df
        except Exception as e:
            print(f"  ERRO ao carregar {nome}: {e}")
            return None
    
    def _validar_dados(self, df: pd.DataFrame, nome: str):
        """Realiza validações nos dados"""
        print(f"\n  Validando: {nome}")
        
        # Verificar valores nulos
        nulos = df.isnull().sum()
        if nulos.sum() > 0:
            print(f"    ⚠ Valores nulos encontrados:")
            for col, count in nulos[nulos > 0].items():
                print(f"      - {col}: {count} valores nulos")
        else:
            print(f"    ✓ Nenhum valor nulo encontrado")
        
        # Verificar duplicados
        duplicados = df.duplicated().sum()
        if duplicados > 0:
            print(f"    ⚠ {duplicados} registros duplicados encontrados")
        else:
            print(f"    ✓ Nenhum registro duplicado")
        
        # Validar campos essenciais
        campos_essenciais = ['REG_ANS', 'CD_CONTA_CONTABIL', 'ANO', 'TRIMESTRE']
        for campo in campos_essenciais:
            if campo in df.columns:
                vazios = df[campo].isna().sum()
                if vazios > 0:
                    print(f"    ⚠ Campo {campo}: {vazios} valores vazios")
                else:
                    print(f"    ✓ Campo {campo}: OK")
            else:
                print(f"    ✗ Campo {campo}: NÃO ENCONTRADO")
    
    def _analise_estatistica(self, df: pd.DataFrame, nome: str):
        """Realiza análise estatística dos dados"""
        print(f"\n  Análise: {nome}")
        print(f"    Total de registros: {len(df)}")
        
        # Análise por ano
        if 'ANO' in df.columns:
            print(f"    Anos disponíveis: {sorted(df['ANO'].unique())}")
        
        # Análise por trimestre
        if 'TRIMESTRE' in df.columns:
            print(f"    Trimestres disponíveis: {sorted(df['TRIMESTRE'].unique())}")
        
        # Análise de valores
        colunas_valores = ['VL_SALDO_INICIAL', 'VL_SALDO_FINAL']
        for col in colunas_valores:
            if col in df.columns:
                serie = pd.to_numeric(
                    df[col].astype(str).str.replace('.', '').str.replace(',', '.'),
                    errors='coerce'
                )
                print(f"    {col}:")
                print(f"      - Mínimo: {serie.min():.2f}")
                print(f"      - Máximo: {serie.max():.2f}")
                print(f"      - Média: {serie.mean():.2f}")
                print(f"      - Mediana: {serie.median():.2f}")
    
    def _importar_dados(self):
        """Importa os dados no banco de dados"""
        try:
            importador = ImportarDadosConsolidados(DATABASE_URL, self.diretorio_dados)
            resultado = importador.executar()
            
            if resultado.registros_importados == 0:
                print("\n⚠ Nenhum registro foi importado")
            else:
                print("\n✓ Importação concluída com sucesso!")
        except Exception as e:
            print(f"\n✗ Erro ao importar dados: {e}")

    def _gerar_despesas_agregadas(self):
        """Gera as despesas agregadas"""
        try:
            gerador = GerarDespesasAgregadas(self.diretorio_dados)
            gerador.executar()
        except Exception as e:
            print(f"\n✗ Erro ao gerar despesas agregadas: {e}")

def principal():
    """Função principal de execução"""
    transformacao = TransformacaoValidacao()
    transformacao.executar()

if __name__ == '__main__':
    principal()
