"""Pipeline de Transformação e Validação (módulo 2)."""

from config import DIRETORIO_CONSOLIDADOS
from casos_uso.gerar_despesas_agregadas import GerarDespesasAgregadas


class TransformacaoPipeline:
    """Orquestra o fluxo de transformação e validação."""

    def __init__(self, diretorio_consolidados: str = DIRETORIO_CONSOLIDADOS) -> None:
        self.diretorio_consolidados = diretorio_consolidados

    def run(self) -> None:
        processador = GerarDespesasAgregadas(self.diretorio_consolidados)
        processador.executar()
