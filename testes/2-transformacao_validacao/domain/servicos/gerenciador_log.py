"""
Serviço responsável pela configuração e gerenciamento de logs.
Centraliza a lógica de preparação de arquivos de log e configuração de loggers.
"""
import os
import zipfile
import logging
from typing import Tuple, Optional
from datetime import datetime


class GerenciadorLog:
    """Gerencia operações de logging"""

    @staticmethod
    def preparar_log_file(
        zip_path: Optional[str],
        nomes_arquivos_zip: list,
        diretorio_saida: str,
    ) -> Tuple[str, Optional[str]]:
        """
        Prepara o arquivo de log, extraindo do ZIP se existir ou criando novo.
        
        Args:
            zip_path: Caminho do arquivo ZIP (pode ser None)
            nomes_arquivos_zip: Lista de arquivos dentro do ZIP
            diretorio_saida: Diretório onde salvar o arquivo de log
        
        Returns:
            Tupla (caminho_log_file, nome_log_no_zip)
        """
        log_dir = os.path.join(diretorio_saida, "logs")
        os.makedirs(log_dir, exist_ok=True)

        log_arquivo_zip_nome = None
        log_file_path = None

        # Se temos um ZIP, tentar extrair o log de dentro dele
        if zip_path and os.path.exists(zip_path):
            try:
                with zipfile.ZipFile(zip_path, "r") as zipf:
                    candidatos = []
                    for nome in nomes_arquivos_zip:
                        nome_lower = nome.lower()
                        if nome_lower.endswith(".log") or ("log" in nome_lower and nome_lower.endswith(".txt")):
                            candidatos.append(nome)

                    if candidatos:
                        log_arquivo_zip_nome = candidatos[0]
                        log_bytes = zipf.read(log_arquivo_zip_nome)
                        nome_arquivo_log = os.path.basename(log_arquivo_zip_nome)
                        log_file_path = os.path.join(log_dir, nome_arquivo_log)
                        with open(log_file_path, "wb") as f:
                            f.write(log_bytes)
            except Exception:
                pass

        # Se não conseguiu extrair, criar um novo arquivo de log
        if not log_file_path:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            nome_arquivo_log = f"validacao_{timestamp}.log"
            log_arquivo_zip_nome = nome_arquivo_log
            log_file_path = os.path.join(log_dir, nome_arquivo_log)

        return log_file_path, log_arquivo_zip_nome

    @staticmethod
    def configurar_logger(log_file_path: str, nome_logger: str = "TransformacaoValidacao") -> logging.Logger:
        """
        Configura um logger com handlers de arquivo e console.
        
        Args:
            log_file_path: Caminho do arquivo de log
            nome_logger: Nome do logger
        
        Returns:
            Logger configurado
        """
        logger = logging.getLogger(nome_logger)
        logger.setLevel(logging.DEBUG)

        # Remover handlers existentes
        for handler in logger.handlers[:]:
            logger.removeHandler(handler)

        formato = logging.Formatter(
            "%(asctime)s | %(levelname)-8s | %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )

        # Handler de arquivo
        fh = logging.FileHandler(log_file_path, encoding="utf-8", mode="a")
        fh.setLevel(logging.DEBUG)
        fh.setFormatter(formato)
        logger.addHandler(fh)

        # Handler de console
        ch = logging.StreamHandler()
        ch.setLevel(logging.INFO)
        ch.setFormatter(formato)
        logger.addHandler(ch)

        return logger
