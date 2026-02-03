"""Ponto de entrada da aplicação de Integração de Dados da API Pública ANS.

Orquestra:
1. Configuração de logging
2. Execução do pipeline de integração (baixar e gerar consolidados)
"""

from casos_uso.configurar_logging import ConfigurarLogging
from casos_uso.baixar_e_gerar_consolidados import BaixarEGerarConsolidados


def principal():
    """Ponto de entrada da aplicação."""
    # 1. Configurar logging (deve ser feito antes de qualquer outro import/log)
    ConfigurarLogging.executar()
    
    # 2. Executar integração completa
    pipeline = BaixarEGerarConsolidados()
    pipeline.executar()


if __name__ == '__main__':
    principal()
