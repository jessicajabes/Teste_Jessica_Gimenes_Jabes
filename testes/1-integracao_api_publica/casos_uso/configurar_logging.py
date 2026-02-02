"""Caso de Uso: Configurar Logging da Aplicação.

Responsável por inicializar e configurar todos os handlers de logging
(console, arquivo de aplicação, log de sessão).
"""

import logging
import os
from datetime import datetime


class ConfigurarLogging:
    """Configura logging para console e arquivo."""
    
    DEFAULT_LOG_DIR = '/app/downloads/logs'
    FALLBACK_LOG_DIR = '/tmp/integracao_logs'
    
    @staticmethod
    def executar(log_dir: str = None) -> str:
        """Configura logging e retorna o caminho do diretório de logs.
        
        Args:
            log_dir: Diretório para logs (usa default se não fornecido)
            
        Returns:
            str: Caminho do diretório de logs configurado
        """
        log_dir = log_dir or ConfigurarLogging.DEFAULT_LOG_DIR
        
        # 1. Criar diretório de logs com fallback
        log_dir = ConfigurarLogging._criar_diretorio_logs(log_dir)
        
        # 2. Criar handlers
        handlers = ConfigurarLogging._criar_handlers(log_dir)
        
        # 3. Configurar logging global
        ConfigurarLogging._configurar_logging_global(handlers)
        
        # 4. Salvar caminho da sessão em variável de ambiente
        ConfigurarLogging._salvar_sessao_atual(log_dir)
        
        return log_dir
    
    @staticmethod
    def _criar_diretorio_logs(log_dir: str) -> str:
        """Cria diretório de logs com fallback se necessário."""
        try:
            os.makedirs(log_dir, exist_ok=True)
            return log_dir
        except Exception as e:
            print(f"Aviso: Não foi possível criar diretório de logs em {log_dir}: {e}")
            
            # Tentar criar com permissões diferentes
            try:
                os.makedirs(log_dir, mode=0o777, exist_ok=True)
                return log_dir
            except Exception:
                # Se ainda assim falhar, usar fallback
                print(f"Usando fallback: {ConfigurarLogging.FALLBACK_LOG_DIR}")
                os.makedirs(ConfigurarLogging.FALLBACK_LOG_DIR, exist_ok=True)
                return ConfigurarLogging.FALLBACK_LOG_DIR
    
    @staticmethod
    def _criar_handlers(log_dir: str) -> list:
        """Cria handlers para logging (console e arquivo)."""
        handlers = [logging.StreamHandler()]  # Console sempre habilitado
        
        # Handler para arquivo de aplicação
        try:
            log_file = os.path.join(log_dir, 'aplicacao.log')
            handlers.append(logging.FileHandler(log_file, encoding='utf-8'))
        except Exception as e:
            print(f"Aviso: Não foi possível criar handler de arquivo: {e}")
        
        # Handler para log da sessão atual
        try:
            session_ts = datetime.now().strftime('%Y%m%d_%H%M%S')
            session_log = os.path.join(log_dir, f'sessao_{session_ts}.log')
            handlers.append(logging.FileHandler(session_log, encoding='utf-8'))
        except Exception as e:
            print(f"Aviso: Não foi possível criar handler de log da sessão: {e}")
        
        return handlers
    
    @staticmethod
    def _configurar_logging_global(handlers: list) -> None:
        """Configura logging global da aplicação."""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s | %(levelname)-8s | %(filename)s:%(lineno)d | %(funcName)s() | %(message)s',
            handlers=handlers,
            force=True  # Force configuração mesmo se logging já foi configurado
        )
    
    @staticmethod
    def _salvar_sessao_atual(log_dir: str) -> None:
        """Salva o caminho do log da sessão atual em variável de ambiente."""
        try:
            session_ts = datetime.now().strftime('%Y%m%d_%H%M%S')
            session_log = os.path.join(log_dir, f'sessao_{session_ts}.log')
            os.environ['LOG_SESSAO_ATUAL'] = session_log
        except Exception:
            pass  # Se falhar, continuar normalmente
