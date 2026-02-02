"""
Caso de Uso: Gerar Despesas Agregadas
Orquestra o processamento de arquivos consolidados, validação, enriquecimento e agregação.
"""
import os
import zipfile

from config import DATABASE_URL, DIRETORIO_CONSOLIDADOS, DIRETORIO_TRANSFORMACAO
from domain.servicos import (
    GerenciadorZIP,
    GerenciadorLog,
    CarregadorDados,
    ValidadorDespesas,
    AgregadorDespesas,
)


class GerarDespesasAgregadas:
    def __init__(self, diretorio_dados: str = DIRETORIO_CONSOLIDADOS):
        self.diretorio_dados = diretorio_dados
        self.diretorio_saida = DIRETORIO_TRANSFORMACAO
        os.makedirs(self.diretorio_saida, exist_ok=True)

        # Localizar ZIP
        nome_base = "consolidado_despesas"  # Nome do arquivo ZIP de entrada
        self.zip_path = GerenciadorZIP.localizar_zip(diretorio_dados, nome_base)
        
        # Preparar log
        nomes_arquivos_zip = []
        if self.zip_path:
            with zipfile.ZipFile(self.zip_path, "r") as zipf:
                nomes_arquivos_zip = zipf.namelist()

        self.log_file_path, self.log_arquivo_zip_nome = GerenciadorLog.preparar_log_file(
            self.zip_path,
            nomes_arquivos_zip,
            self.diretorio_saida,
        )
        
        # Configurar logger
        self.logger = GerenciadorLog.configurar_logger(self.log_file_path)

        # Arquivos a processar
        self.arquivo_sinistros_sem_deducoes = "sinistro_sem_deducoes.csv"
        self.arquivo_sinistros_c_deducoes = "consolidado_despesas_sinistros_c_deducoes.csv"

        # Caminhos de saída
        self.arquivo_saida_sem_deducoes = os.path.join(self.diretorio_saida, "despesas_agregadas.csv")
        self.arquivo_saida_c_deducoes = os.path.join(self.diretorio_saida, "despesas_agregadas_c_deducoes.csv")

    def executar(self):
        """Executa o processamento completo de validação e agregação"""
        print("=" * 60)
        print("VALIDAÇÃO E AGREGAÇÃO DE DESPESAS")
        print("=" * 60)

        # Carregar operadoras
        operadoras = CarregadorDados.carregar_operadoras(DATABASE_URL, self.logger)

        # Processar arquivo sem deduções
        agreg_sem = None
        df_sem = CarregadorDados.carregar_despesas(
            self.arquivo_sinistros_sem_deducoes,
            self.zip_path,
            self.diretorio_dados,
            self.logger,
        )
        
        if df_sem is not None:
            df_sem_validado = ValidadorDespesas.validar_e_enriquecer(
                df_sem, 
                operadoras, 
                "sinistro_sem_deducoes", 
                self.logger
            )
            
            agreg_sem = AgregadorDespesas.agregar_por_operadora_uf(df_sem_validado)

        # Processar arquivo com deduções
        agreg_c_deducoes = None
        df_c_deducoes = CarregadorDados.carregar_despesas(
            self.arquivo_sinistros_c_deducoes,
            self.zip_path,
            self.diretorio_dados,
            self.logger,
        )
        if df_c_deducoes is not None:
            df_c_deducoes = ValidadorDespesas.validar_e_enriquecer(
                df_c_deducoes,
                operadoras,
                "consolidado_c_deducoes",
                self.logger,
            )
            agreg_c_deducoes = AgregadorDespesas.agregar_por_operadora_uf(df_c_deducoes)


        # Criar novo ZIP com os arquivos agregados
        GerenciadorZIP.criar_zip_com_dataframes(
            self.diretorio_saida,
            agreg_sem,
            agreg_c_deducoes,
            self.log_file_path,
        )

        print("=" * 60)
        print("PROCESSAMENTO CONCLUÍDO")
        print("=" * 60)
