"""Ponto de entrada da aplicação de Integração de Dados da API Pública ANS.

Orquestra:
1. Configuração de logging
2. Execução do pipeline de integração
"""

from casos_uso.configurar_logging import ConfigurarLogging
from casos_uso.gerar_arquivos_consolidados import GerarArquivosConsolidados


def principal():
    """Ponto de entrada da aplicação."""
    # 1. Configurar logging (deve ser feito antes de qualquer outro import/log)
    ConfigurarLogging.executar()
    
    # 2. Executar integração
    gerar_consolidados = GerarArquivosConsolidados()
    gerar_consolidados.executar()


if __name__ == '__main__':
    principal()
