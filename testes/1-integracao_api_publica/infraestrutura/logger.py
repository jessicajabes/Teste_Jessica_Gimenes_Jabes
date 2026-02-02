"""
Sistema de logging centralizado para o projeto
"""

import os
import logging
from datetime import datetime
from config import DIRETORIO_DOWNLOADS

class LoggerConfig:
    _logger = None
    _log_dir = os.path.join(DIRETORIO_DOWNLOADS, 'logs')
    
    @classmethod
    def get_logger(cls, nome: str = 'app') -> logging.Logger:
        """Retorna o logger configurado"""
        if cls._logger is None:
            cls._configurar_logger(nome)
        return cls._logger
    
    @classmethod
    def _configurar_logger(cls, nome: str):
        """Configura o logger com handlers para arquivo e console"""
        # Criar diretório de logs se não existir
        try:
            os.makedirs(cls._log_dir, exist_ok=True)
        except Exception as e:
            print(f"Aviso: Não foi possível criar diretório de logs {cls._log_dir}: {e}")
            # Usar /tmp como fallback
            cls._log_dir = '/tmp/integracao_logs'
            try:
                os.makedirs(cls._log_dir, exist_ok=True)
            except:
                cls._log_dir = '/tmp'
        
        # Criar logger
        cls._logger = logging.getLogger(nome)
        cls._logger.setLevel(logging.DEBUG)
        
        # Limpar handlers existentes
        cls._logger.handlers = []
        
        # Formatar mensagens com informações de arquivo e linha
        formato_detalhado = logging.Formatter(
            '%(asctime)s | %(levelname)-8s | %(filename)s:%(lineno)d | %(funcName)s() | %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        
        # Handler para arquivo (arquivo consolidado de todas as operações)
        try:
            arquivo_log = os.path.join(cls._log_dir, 'aplicacao.log')
            fh = logging.FileHandler(arquivo_log, encoding='utf-8')
            fh.setLevel(logging.DEBUG)
            fh.setFormatter(formato_detalhado)
            cls._logger.addHandler(fh)
        except Exception as e:
            print(f"Aviso: Não foi possível criar handler de arquivo de log: {e}")
        
        # Handler para console (menos detalhado)
        ch = logging.StreamHandler()
        ch.setLevel(logging.INFO)
        formatter_console = logging.Formatter(
            '%(asctime)s | %(levelname)-8s | %(message)s',
            datefmt='%H:%M:%S'
        )
        ch.setFormatter(formatter_console)
        cls._logger.addHandler(ch)

def get_logger(nome: str = 'app') -> logging.Logger:
    """Função auxiliar para obter o logger"""
    return LoggerConfig.get_logger(nome)
