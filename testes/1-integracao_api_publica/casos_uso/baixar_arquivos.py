from typing import List, Optional

from config import API_BASE_URL, DIRETORIO_ZIPS
from domain.entidades import Trimestre, Arquivo
from domain.repositorios import RepositorioAPI
from infraestrutura.repositorio_api_http import RepositorioAPIHTTP

class BaixarArquivosTrimestres:
    def __init__(self, repositorio: Optional[RepositorioAPI] = None, diretorio_destino: str = None):
        if repositorio is None:
            self.repositorio_api_http = RepositorioAPIHTTP(API_BASE_URL)
            self._repositorio_interno = True
        else:
            self.repositorio_api_http = repositorio
            self._repositorio_interno = False
        self.diretorio_destino = diretorio_destino or DIRETORIO_ZIPS
    
    def executar(self, trimestres: List[Trimestre]) -> List[Arquivo]:
        arquivos_baixados = []

        try:
            for trimestre in trimestres:
                print(f"\nTrimestre {trimestre}:")
                caminhos_arquivos = self.repositorio_api_http.obter_arquivos_do_trimestre(trimestre)

                if caminhos_arquivos:
                    print(f"  {len(caminhos_arquivos)} arquivo(s) encontrado(s)")

                    for caminho in caminhos_arquivos:
                        arquivo = Arquivo(
                            nome=caminho,
                            caminho=caminho,
                            trimestre=trimestre,
                        )

                        sucesso = self.repositorio_api_http.baixar_arquivo(arquivo, self.diretorio_destino)
                        if sucesso:
                            arquivos_baixados.append(arquivo)
                else:
                    print("  Nenhum arquivo encontrado")
        finally:
            if self._repositorio_interno:
                self.repositorio_api_http.fechar()

        return arquivos_baixados
