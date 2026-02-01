"""
Main - Transformação e Validação de Dados

Processa os arquivos consolidados (a partir do ZIP),
realiza validações, enriquece com dados de operadoras
e gera agregações.
"""

from config import DIRETORIO_CONSOLIDADOS
from casos_uso.gerar_despesas_agregadas import GerarDespesasAgregadas


def principal():
    """Função principal de execução"""
    processador = GerarDespesasAgregadas(DIRETORIO_CONSOLIDADOS)
    processador.executar()


if __name__ == '__main__':
    principal()
