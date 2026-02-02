"""Ponto de entrada da aplicação de Transformação e Validação de Dados.

Processa os arquivos consolidados (a partir do ZIP),
realiza validações, enriquece com dados de operadoras
e gera agregações.
"""

from casos_uso.gerar_despesas_agregadas import GerarDespesasAgregadas


def principal():
    """Ponto de entrada da aplicação."""
    processador = GerarDespesasAgregadas()
    processador.executar()


if __name__ == '__main__':
    principal()
