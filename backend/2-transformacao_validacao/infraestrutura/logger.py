"""
Sistema de logging centralizado para o projeto
"""

import os
import logging
from datetime import datetime

class LoggerConfig:
    _logger = None
    _log_dir = None
    _initialized = False
    
    @classmethod
    def set_log_dir(cls, log_dir: str):
        """Define o diretório de logs e reinicializa o logger"""
        cls._log_dir = log_dir
        cls._initialized = False  # Forçar reinicialização
        cls._logger = None
    
    @classmethod
    def get_logger(cls, nome: str = 'app') -> logging.Logger:
        """Retorna o logger configurado"""
        if cls._logger is None or not cls._initialized:
            cls._configurar_logger(nome)
        return cls._logger
    
    @classmethod
    def _configurar_logger(cls, nome: str):
        """Configura o logger com handlers para arquivo e console"""
        # Se log_dir não foi definido, usar padrão
        if cls._log_dir is None:
            cls._log_dir = './logs'
        
        # Criar diretório de logs se não existir
        os.makedirs(cls._log_dir, exist_ok=True)
        
        # Criar logger
        cls._logger = logging.getLogger(nome)
        cls._logger.setLevel(logging.DEBUG)
        
        # Limpar handlers existentes
        for handler in cls._logger.handlers[:]:
            cls._logger.removeHandler(handler)
        
        # Formatar mensagens com informações de arquivo e linha
        formato_detalhado = logging.Formatter(
            '%(asctime)s | %(levelname)-8s | %(filename)s:%(lineno)d | %(funcName)s() | %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        
        # Handler para arquivo (arquivo consolidado de todas as operações)
        arquivo_log = os.path.join(cls._log_dir, 'aplicacao.log')
        fh = logging.FileHandler(arquivo_log, encoding='utf-8')
        fh.setLevel(logging.DEBUG)
        fh.setFormatter(formato_detalhado)
        cls._logger.addHandler(fh)
        
        # Handler para console (menos detalhado)
        ch = logging.StreamHandler()
        ch.setLevel(logging.INFO)
        formatter_console = logging.Formatter(
            '%(asctime)s | %(levelname)-8s | %(message)s',
            datefmt='%H:%M:%S'
        )
        ch.setFormatter(formatter_console)
        cls._logger.addHandler(ch)
        
        cls._initialized = True

def get_logger(nome: str = 'app') -> logging.Logger:
    """Função auxiliar para obter o logger"""
    logger = LoggerConfig.get_logger(nome)
    # Forçar flush imediato
    for handler in logger.handlers:
        if isinstance(handler, logging.FileHandler):
            handler.flush()
    return logger
