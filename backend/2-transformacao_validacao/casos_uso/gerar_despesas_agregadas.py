"""
Gerador de Despesas Agregadas

Processa os arquivos CSV consolidados e gera análises agregadas por:
- Razão Social e UF
- Total de despesas por operadora e por UF
- Média de despesas por trimestre
- Desvio padrão das despesas
"""

import os
import pandas as pd
import numpy as np
from typing import Dict, List
from config import DIRETORIO_CONSOLIDADOS, DIRETORIO_TRANSFORMACAO
from infraestrutura.logger import get_logger

class GerarDespesasAgregadas:
    """Gera análises agregadas de despesas"""
    
    def __init__(self, diretorio_dados: str = DIRETORIO_CONSOLIDADOS):
        self.logger = get_logger('GerarDespesasAgregadas')
        self.diretorio_dados = diretorio_dados
        self.diretorio_saida = DIRETORIO_TRANSFORMACAO
        self.arquivo_despesas_sinistros = os.path.join(diretorio_dados, "consolidado_despesas_sinistros.csv")
        self.arquivo_todas_despesas = os.path.join(diretorio_dados, "consolidado_todas_despesas.csv")
        self.arquivo_saida = os.path.join(self.diretorio_saida, "despesas_agregadas.csv")
        self.arquivo_zip = os.path.join(self.diretorio_saida, "despesas_agregadas.zip")
        
        # Criar diretório de saída se não existir
        os.makedirs(self.diretorio_saida, exist_ok=True)
    
    def executar(self):
        """Executa o processo de agregação"""
        print("="*60)
        print("GERAÇÃO DE DESPESAS AGREGADAS")
        print("="*60)
        
        resultados = []
        
        # Processar Despesas com Sinistros
        print("\n1. Processando Despesas com Sinistros...")
        if os.path.exists(self.arquivo_despesas_sinistros):
            df_sinistros = self._carregar_csv(self.arquivo_despesas_sinistros)
            if df_sinistros is not None and not df_sinistros.empty:
                resultado_sinistros = self._processar_arquivo(
                    df_sinistros, 
                    "Despesas com Sinistros"
                )
                resultados.extend(resultado_sinistros)
        else:
            print(f"  ✗ Arquivo não encontrado: {self.arquivo_despesas_sinistros}")
        
        # Processar Todas as Despesas
        print("\n2. Processando Todas as Despesas...")
        if os.path.exists(self.arquivo_todas_despesas):
            df_todas = self._carregar_csv(self.arquivo_todas_despesas)
            if df_todas is not None and not df_todas.empty:
                resultado_todas = self._processar_arquivo(
                    df_todas,
                    "Todas as Despesas"
                )
                resultados.extend(resultado_todas)
        else:
            print(f"  ✗ Arquivo não encontrado: {self.arquivo_todas_despesas}")
        
        # Salvar resultados
        if resultados:
            self._salvar_resultados(resultados)
            self._gerar_zip()
            print(f"\n✓ Análise concluída com sucesso!")
            print(f"✓ {len(resultados)} registros agregados salvos em: {self.arquivo_saida}")
            print(f"✓ Arquivo ZIP gerado: {self.arquivo_zip}")
        else:
            print("\n⚠ Nenhum dado agregado foi gerado")
        
        print("="*60 + "\n")
    
    def _carregar_csv(self, caminho: str) -> pd.DataFrame:
        """Carrega um arquivo CSV"""
        try:
            df = pd.read_csv(caminho, sep=';', encoding='utf-8')
            print(f"  ✓ Carregado: {len(df)} registros")
            return df
        except Exception as e:
            print(f"  ✗ Erro ao carregar CSV: {e}")
            return None
    
    def _processar_arquivo(self, df: pd.DataFrame, tipo_despesa: str) -> List[Dict]:
        """Processa um arquivo e gera agregações"""
        resultados = []
        
        # Normalizar nomes de colunas
        df.columns = df.columns.str.upper().str.strip()
        
        # Verificar se as colunas necessárias existem
        colunas_necessarias = ['DESCRICAO', 'VL_SALDO_INICIAL', 'VL_SALDO_FINAL', 'TRIMESTRE', 'ANO']
        
        # Verificar se existe coluna de UF (pode ter nomes diferentes)
        col_uf = None
        for col in ['UF', 'SG_UF', 'SIGLA_UF', 'ESTADO']:
            if col in df.columns:
                col_uf = col
                break
        
        if col_uf is None:
            self.logger.warning(f"Coluna UF não encontrada em '{tipo_despesa}'. Usando 'N/A' como padrão")
            print(f"  ⚠ Coluna UF não encontrada. Usando 'N/A' como padrão")
            df['UF'] = 'N/A'
            col_uf = 'UF'
        
        # Verificar colunas obrigatórias
        for col in colunas_necessarias:
            if col not in df.columns:
                self.logger.error(f"Coluna obrigatória não encontrada em '{tipo_despesa}': {col}")
                print(f"  ✗ Coluna obrigatória não encontrada: {col}")
                return []
        
        # Usar DESCRICAO como Razão Social
        df['RAZAO_SOCIAL'] = df['DESCRICAO']
        
        # Converter valores para numérico
        df['VL_SALDO_INICIAL'] = pd.to_numeric(
            df['VL_SALDO_INICIAL'].astype(str).str.replace(',', '.'),
            errors='coerce'
        )
        df['VL_SALDO_FINAL'] = pd.to_numeric(
            df['VL_SALDO_FINAL'].astype(str).str.replace(',', '.'),
            errors='coerce'
        )
        
        # Calcular total de despesas (usando VL_SALDO_FINAL como referência)
        df['TOTAL_DESPESAS'] = df['VL_SALDO_FINAL'].fillna(0)
        
        # Remover registros com valores inválidos
        df = df[df['TOTAL_DESPESAS'] != 0]
        
        if df.empty:
            self.logger.warning(f"Nenhum registro válido após limpeza em '{tipo_despesa}'")
            print(f"  ⚠ Nenhum registro válido após limpeza")
            return []
        
        print(f"\n  Agregando dados por Razão Social e UF...")
        
        # Agrupar por Razão Social e UF
        agrupamento = df.groupby(['RAZAO_SOCIAL', col_uf]).agg({
            'TOTAL_DESPESAS': ['sum', 'mean', 'std', 'count'],
            'TRIMESTRE': 'nunique',
            'ANO': 'nunique'
        }).reset_index()
        
        # Renomear colunas
        agrupamento.columns = [
            'razao_social',
            'uf',
            'total_despesas',
            'media_despesas_trimestre',
            'desvio_padrao_despesas',
            'qtd_registros',
            'qtd_trimestres',
            'qtd_anos'
        ]
        
        # Substituir NaN no desvio padrão por 0
        agrupamento['desvio_padrao_despesas'] = agrupamento['desvio_padrao_despesas'].fillna(0)
        
        # Ordenar do maior para o menor total de despesas
        agrupamento = agrupamento.sort_values('total_despesas', ascending=False)
        
        print(f"  ✓ {len(agrupamento)} grupos agregados")
        print(f"  ✓ Total geral de despesas: R$ {agrupamento['total_despesas'].sum():,.2f}")
        
        # Converter para lista de dicionários e adicionar tipo
        for _, row in agrupamento.iterrows():
            resultado = {
                'tipo_despesa': tipo_despesa,
                'razao_social': row['razao_social'],
                'uf': row['uf'],
                'total_despesas': round(row['total_despesas'], 2),
                'media_despesas_trimestre': round(row['media_despesas_trimestre'], 2),
                'desvio_padrao_despesas': round(row['desvio_padrao_despesas'], 2),
                'qtd_registros': int(row['qtd_registros']),
                'qtd_trimestres': int(row['qtd_trimestres']),
                'qtd_anos': int(row['qtd_anos'])
            }
            resultados.append(resultado)
        
        return resultados
    
    def _salvar_resultados(self, resultados: List[Dict]):
        """Salva os resultados em CSV"""
        try:
            df_resultado = pd.DataFrame(resultados)
            
            # Ordenar novamente do maior para o menor
            df_resultado = df_resultado.sort_values('total_despesas', ascending=False)
            
            # Salvar CSV
            df_resultado.to_csv(self.arquivo_saida, index=False, encoding='utf-8', sep=';')
            
            # Exibir resumo
            print(f"\n  === RESUMO DOS RESULTADOS ===")
            print(f"  Total de registros agregados: {len(df_resultado)}")
            print(f"  Total geral de despesas: R$ {df_resultado['total_despesas'].sum():,.2f}")
            print(f"  Média geral: R$ {df_resultado['media_despesas_trimestre'].mean():,.2f}")
            print(f"  Desvio padrão médio: R$ {df_resultado['desvio_padrao_despesas'].mean():,.2f}")
            
            # Top 5
            print(f"\n  === TOP 5 MAIORES DESPESAS ===")
            top5 = df_resultado.head(5)
            for idx, row in top5.iterrows():
                print(f"  {row['razao_social'][:40]} ({row['uf']}) - R$ {row['total_despesas']:,.2f}")
            
        except Exception as e:
            print(f"  ✗ Erro ao salvar resultados: {e}")
    
    def _gerar_zip(self):
        """Gera arquivo ZIP com despesas agregadas e erros"""
        try:
            import zipfile
            
            # Lista de arquivos para compactar
            arquivos_para_zip = []
            
            # Adicionar despesas_agregadas.csv
            if os.path.exists(self.arquivo_saida):
                arquivos_para_zip.append((
                    self.arquivo_saida,
                    os.path.basename(self.arquivo_saida)
                ))
            
            # Adicionar erros_insercao.csv se existir
            arquivo_erros = os.path.join(self.diretorio_saida, "erros_insercao.csv")
            if os.path.exists(arquivo_erros):
                arquivos_para_zip.append((
                    arquivo_erros,
                    os.path.basename(arquivo_erros)
                ))
            
            if not arquivos_para_zip:
                print("  ⚠ Nenhum arquivo para compactar")
                return
            
            # Criar arquivo ZIP
            with zipfile.ZipFile(self.arquivo_zip, 'w', zipfile.ZIP_DEFLATED) as zipf:
                for caminho_completo, nome_arquivo in arquivos_para_zip:
                    zipf.write(caminho_completo, nome_arquivo)
                    print(f"  ✓ Adicionado ao ZIP: {nome_arquivo}")
            
            # Exibir tamanho do arquivo
            tamanho_zip = os.path.getsize(self.arquivo_zip) / 1024  # KB
            print(f"  ✓ Arquivo ZIP criado: {tamanho_zip:.2f} KB")
            
        except Exception as e:
            print(f"  ✗ Erro ao gerar ZIP: {e}")
