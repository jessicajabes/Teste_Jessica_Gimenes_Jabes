from typing import List
from domain.entidades import Trimestre, Arquivo
from domain.repositorios import RepositorioAPI
from config import DIRETORIO_ZIPS

class BaixarArquivosTrimestres:
    def __init__(self, repositorio: RepositorioAPI, diretorio_destino: str = None):
        self.repositorio = repositorio
        self.diretorio_destino = diretorio_destino or DIRETORIO_ZIPS
    
    def executar(self, trimestres: List[Trimestre]) -> List[Arquivo]:
        arquivos_baixados = []
        
        for trimestre in trimestres:
            print(f"\nTrimestre {trimestre}:")
            caminhos_arquivos = self.repositorio.obter_arquivos_do_trimestre(trimestre)
            
            if caminhos_arquivos:
                print(f"  {len(caminhos_arquivos)} arquivo(s) encontrado(s)")
                
                for caminho in caminhos_arquivos:
                    arquivo = Arquivo(
                        nome=caminho,
                        caminho=caminho,
                        trimestre=trimestre
                    )
                    
                    sucesso = self.repositorio.baixar_arquivo(arquivo, self.diretorio_destino)
                    if sucesso:
                        arquivos_baixados.append(arquivo)
            else:
                print("  Nenhum arquivo encontrado")
        
        return arquivos_baixados
