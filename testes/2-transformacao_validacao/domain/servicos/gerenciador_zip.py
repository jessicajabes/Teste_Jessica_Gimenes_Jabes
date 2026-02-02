"""
Serviço responsável por todas as operações relacionadas a arquivos ZIP.
Centraliza a lógica de localização, leitura, criação e atualização de arquivos ZIP.
"""
import os
import re
import io
import zipfile
from typing import Optional, List
import pandas as pd


class GerenciadorZIP:
    """Gerencia operações de arquivos ZIP"""

    @staticmethod
    def localizar_zip(diretorio: str, nome_base: str) -> Optional[str]:
        """
        Localiza um arquivo ZIP no diretório baseado no nome base.
        
        Args:
            diretorio: Diretório onde procurar o ZIP
            nome_base: Nome base do arquivo (ex: "despesas_5T2024")
        
        Returns:
            Caminho completo do ZIP encontrado ou None
        """
        if not os.path.isdir(diretorio):
            return None

        padrao = re.escape(nome_base) + r"\.zip$"
        for arq in os.listdir(diretorio):
            if re.match(padrao, arq, re.IGNORECASE):
                return os.path.join(diretorio, arq)
        return None

    @staticmethod
    def ler_csv_do_zip(caminho_zip: str, nome_arquivo: str) -> Optional[pd.DataFrame]:
        """
        Lê um arquivo CSV de dentro de um ZIP.
        
        Args:
            caminho_zip: Caminho completo do arquivo ZIP
            nome_arquivo: Nome do arquivo CSV dentro do ZIP
        
        Returns:
            DataFrame com o conteúdo do CSV ou None se não encontrado
        """
        try:
            with zipfile.ZipFile(caminho_zip, "r") as zipf:
                if nome_arquivo not in zipf.namelist():
                    return None
                with zipf.open(nome_arquivo) as arquivo:
                    conteudo = arquivo.read()
                    return pd.read_csv(io.BytesIO(conteudo), sep=";", encoding="utf-8-sig")
        except Exception:
            return None

    @staticmethod
    def encontrar_log_zip(nomes: List[str]) -> Optional[str]:
        """
        Encontra o primeiro arquivo de log em uma lista de nomes.
        
        Args:
            nomes: Lista de nomes de arquivos
        
        Returns:
            Nome do arquivo de log encontrado ou None
        """
        candidatos = []
        for nome in nomes:
            nome_lower = nome.lower()
            if nome_lower.endswith(".log") or ("log" in nome_lower and nome_lower.endswith(".txt")):
                candidatos.append(nome)
        return candidatos[0] if candidatos else None

    @staticmethod
    def criar_zip_com_dataframes(
        caminho_destino: str,
        agreg_sem_deducoes,
        agreg_c_deducoes,
        log_file_path: str,
    ) -> bool:
        """
        Cria um novo ZIP com os DataFrames agregados e log.
        Os DataFrames são convertidos em CSV em memória e adicionados ao ZIP.
        Os valores monetários são formatados para o padrão brasileiro.
        
        Args:
            caminho_destino: Caminho onde criar o ZIP (diretório)
            agreg_sem_deducoes: DataFrame agregado sem deduções
            agreg_c_deducoes: DataFrame agregado com deduções
            log_file_path: Caminho do arquivo de log
        
        Returns:
            True se criado com sucesso, False caso contrário
        """
        import pandas as pd
        
        def formatar_moeda_brasileira(valor):
            """Formata valor como moeda brasileira: 1234567.89 -> 1.234.567,89"""
            if pd.isna(valor):
                return ""
            # Formata com 2 casas decimais e separador de milhares (,)
            # Depois inverte pontos e vírgulas para padrão brasileiro
            return f"{valor:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
        
        caminho_zip = os.path.join(caminho_destino, "Teste_Jessica_Jabes.zip")
        try:
            with zipfile.ZipFile(caminho_zip, "w", zipfile.ZIP_DEFLATED) as zipf:
                # Adicionar CSV sem deduções se existir
                if agreg_sem_deducoes is not None:
                    df_formato = agreg_sem_deducoes.copy()
                    # Formatar colunas numéricas
                    colunas_numericas = ["total_despesas", "media_despesas_trimestre", "desvio_padrao_despesas"]
                    for col in colunas_numericas:
                        if col in df_formato.columns:
                            df_formato[col] = df_formato[col].apply(formatar_moeda_brasileira)
                    
                    csv_content = df_formato.to_csv(sep=";", index=False)
                    # Adicionar BOM UTF-8 para Excel detectar encoding automaticamente
                    csv_bytes = b'\xef\xbb\xbf' + csv_content.encode('utf-8')
                    zipf.writestr("despesas_agregadas.csv", csv_bytes)

                # Adicionar CSV com deduções se existir
                if agreg_c_deducoes is not None:
                    df_formato = agreg_c_deducoes.copy()
                    # Formatar colunas numéricas
                    colunas_numericas = ["total_despesas", "media_despesas_trimestre", "desvio_padrao_despesas"]
                    for col in colunas_numericas:
                        if col in df_formato.columns:
                            df_formato[col] = df_formato[col].apply(formatar_moeda_brasileira)
                    
                    csv_content = df_formato.to_csv(sep=";", index=False)
                    # Adicionar BOM UTF-8 para Excel detectar encoding automaticamente
                    csv_bytes = b'\xef\xbb\xbf' + csv_content.encode('utf-8')
                    zipf.writestr("despesas_agregadas_c_deducoes.csv", csv_bytes)

                # Adicionar o log se existir
                if os.path.exists(log_file_path):
                    with open(log_file_path, 'r', encoding='utf-8') as f:
                        conteudo = f.read()
                    zipf.writestr(os.path.join("logs", os.path.basename(log_file_path)), conteudo)

            print(f"✓ ZIP gerado: {caminho_zip}")
            return True
            
        except Exception as e:
            print(f"Erro ao criar ZIP de transformação: {e}")
            return False

    @staticmethod
    def criar_zip_com_logs(
        caminho_destino: str,
        arquivo_saida_sem_deducoes: str,
        arquivo_saida_c_deducoes: str,
        log_file_path: str,
    ) -> bool:
        """
        Cria um novo ZIP com os arquivos CSV agregados e log.
        Os CSVs são lidos do disco e adicionados ao ZIP.
        
        Args:
            caminho_destino: Caminho onde criar o ZIP (diretório)
            arquivo_saida_sem_deducoes: Caminho do CSV sem deduções
            arquivo_saida_c_deducoes: Caminho do CSV com deduções
            log_file_path: Caminho do arquivo de log
        
        Returns:
            True se criado com sucesso, False caso contrário
        """
        arquivos_csv = [arquivo_saida_sem_deducoes, arquivo_saida_c_deducoes]
        
        caminho_zip = os.path.join(caminho_destino, "despesas_agregadas.zip")
        try:
            with zipfile.ZipFile(caminho_zip, "w", zipfile.ZIP_DEFLATED) as zipf:
                # Adicionar CSVs (apenas os que existem)
                for caminho in arquivos_csv:
                    if os.path.exists(caminho):
                        # Ler o CSV e adicionar ao ZIP
                        with open(caminho, 'r', encoding='utf-8') as f:
                            conteudo = f.read()
                        zipf.writestr(os.path.basename(caminho), conteudo)

                # Adicionar o log se existir
                if os.path.exists(log_file_path):
                    with open(log_file_path, 'r', encoding='utf-8') as f:
                        conteudo = f.read()
                    zipf.writestr(os.path.join("logs", os.path.basename(log_file_path)), conteudo)

            print(f"✓ ZIP gerado: {caminho_zip}")
            return True
            
        except Exception as e:
            print(f"Erro ao criar ZIP de transformação: {e}")
            return False
