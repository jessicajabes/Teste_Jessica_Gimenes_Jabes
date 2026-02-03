"""Caso de Uso: Baixar Arquivos ZIP dos Trimestres."""

from typing import List, Optional

from config import API_BASE_URL, DIRETORIO_ZIPS
from domain.entidades import Trimestre, Arquivo
from domain.repositorios import RepositorioAPI
from infraestrutura.cliente_api_ans import ClienteAPIANS


class BaixarArquivosTrimestres:
    """Baixa arquivos ZIP dos trimestres da API ANS.
    
    Para cada trimestre:
        1. Lista arquivos disponíveis
        2. Baixa cada arquivo ZIP
        3. Salva no diretório configurado
    """
    
    def __init__(self, repositorio: Optional[RepositorioAPI] = None, diretorio_destino: str = None):
        """Inicializa downloader de arquivos.
        
        Args:
            repositorio: Cliente da API (opcional, cria um novo se None)
            diretorio_destino: Diretório para salvar ZIPs (padrão: config.DIRETORIO_ZIPS)
        """
        if repositorio is None:
            self.repositorio_api = ClienteAPIANS(API_BASE_URL)
            self._repositorio_interno = True
        else:
            self.repositorio_api = repositorio
            self._repositorio_interno = False
        
        self.diretorio_destino = diretorio_destino or DIRETORIO_ZIPS
    
    def executar(self, trimestres: List[Trimestre]) -> List[Arquivo]:
        """Baixa arquivos de todos os trimestres.
        
        Args:
            trimestres: Lista de trimestres para baixar
            
        Returns:
            Lista de arquivos baixados com sucesso
        """
        arquivos_baixados = []

        try:
            for trimestre in trimestres:
                print(f"\nTrimestre {trimestre}:")
                
                # Listar arquivos disponíveis no trimestre
                caminhos_arquivos = self.repositorio_api.obter_arquivos_do_trimestre(trimestre)

                if not caminhos_arquivos:
                    print("   Nenhum arquivo encontrado")
                    continue
                
                print(f"  {len(caminhos_arquivos)} arquivo(s) encontrado(s)")

                # Baixar cada arquivo
                for caminho in caminhos_arquivos:
                    arquivo = Arquivo(
                        nome=caminho,
                        caminho=caminho,
                        trimestre=trimestre,
                    )

                    sucesso = self.repositorio_api.baixar_arquivo(arquivo, self.diretorio_destino)
                    if sucesso:
                        arquivos_baixados.append(arquivo)
                        print(f"    [OK] {arquivo.nome}")
                    else:
                        print(f"    [ERRO] Falha ao baixar {arquivo.nome}")
        
        finally:
            # Fechar conexão se foi criada internamente
            if self._repositorio_interno:
                self.repositorio_api.fechar()

        return arquivos_baixados
