"""
Main - Transformação e Validação de Dados

Processa os arquivos consolidados (a partir do ZIP),
realiza validações, enriquece com dados de operadoras
e gera agregações.
"""

from pipelines.transformacao_pipeline import TransformacaoPipeline


def principal():
    """Função principal de execução"""
    pipeline = TransformacaoPipeline()
    pipeline.run()


if __name__ == '__main__':
    principal()
