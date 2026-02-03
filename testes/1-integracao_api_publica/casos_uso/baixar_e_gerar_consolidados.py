"""Caso de Uso Principal: Baixar e Gerar Consolidados.

Orquestra todo o fluxo de integração:
1. Buscar trimestres disponíveis na API ANS
2. Baixar arquivos ZIP dos trimestres
3. Extrair arquivos CSV dos ZIPs
4. Gerar CSVs consolidados com JOIN pandas (sem banco de dados)
"""

import os
from typing import Dict

from config import DIRETORIO_DOWNLOADS, DIRETORIO_CONSOLIDADO
from casos_uso.buscar_trimestres_disponiveis import BuscarTrimestresDisponiveis
from casos_uso.baixar_arquivos_trimestres import BaixarArquivosTrimestres
from infraestrutura.gerenciador_arquivos import GerenciadorArquivos
from domain.servicos.gerador_consolidados_pandas import GeradorConsolidadosPandas
from infraestrutura.logger import get_logger

logger = get_logger("BaixarEGerarConsolidados")


class BaixarEGerarConsolidados:
    """Orquestra o fluxo completo de integração de dados da API ANS."""
    
    def executar(self) -> Dict:
        """Executa todo o pipeline de integração.
        
        Fluxo:
            1. Busca últimos 3 trimestres disponíveis
            2. Baixa arquivos ZIP de cada trimestre
            3. Extrai arquivos CSV dos ZIPs
            4. Gera consolidados via pandas JOIN (operadoras + despesas)
        
        Returns:
            Dict com resultado do processamento:
                - sucesso: bool
                - registros: int
                - com_operadora: int
                - sem_operadora: int
                - arquivos_gerados: List[str]
        """
        logger.info("=" * 60)
        logger.info("INICIANDO INTEGRAÇÃO API ANS")
        logger.info("=" * 60)

        print("=" * 60)
        print("INTEGRAÇÃO DE DADOS - API PÚBLICA ANS")
        print("=" * 60)

        # PASSO 1: Buscar trimestres disponíveis
        print("\n[1/4] Buscando trimestres disponíveis...")
        buscar_trimestres = BuscarTrimestresDisponiveis()
        trimestres = buscar_trimestres.executar()
        
        if not trimestres:
            print("⚠ Nenhum trimestre encontrado")
            logger.warning("Nenhum trimestre encontrado")
            return self._resultado_vazio("Nenhum trimestre encontrado")

        print(f"✓ Encontrados {len(trimestres)} trimestres:")
        for trimestre in trimestres:
            print(f"  • {trimestre}")

        # PASSO 2: Baixar arquivos ZIP
        print(f"\n[2/4] Baixando arquivos de {len(trimestres)} trimestres...")
        baixar_arquivos = BaixarArquivosTrimestres()
        arquivos_baixados = baixar_arquivos.executar(trimestres)
        
        if not arquivos_baixados:
            print("✗ Nenhum arquivo foi baixado")
            logger.error("Falha ao baixar arquivos")
            return self._resultado_erro("Nenhum arquivo foi baixado")

        print(f"✓ {len(arquivos_baixados)} arquivos baixados com sucesso")

        # PASSO 3: Extrair ZIPs
        print("\n[3/4] Extraindo arquivos CSV dos ZIPs...")
        gerenciador_arquivos = GerenciadorArquivos()
        from config import DIRETORIO_ZIPS
        gerenciador_arquivos.extrair_zips(DIRETORIO_ZIPS)
        print("✓ Arquivos extraídos")

        # PASSO 4: Gerar consolidados via pandas JOIN
        print("\n[4/4] Gerando arquivos consolidados...")
        gerador = GeradorConsolidadosPandas()
        
        diretorio_consolidados = os.path.join(DIRETORIO_DOWNLOADS, DIRETORIO_CONSOLIDADO)
        os.makedirs(diretorio_consolidados, exist_ok=True)
        
        # Obter arquivo de log da sessão atual
        from infraestrutura.logger import obter_arquivo_log_sessao
        arquivo_log = obter_arquivo_log_sessao()
        
        resultado = gerador.gerar_consolidados_com_join(
            diretorio_origem=DIRETORIO_DOWNLOADS,
            diretorio_destino=diretorio_consolidados,
            arquivo_log=arquivo_log
        )

        # PASSO 5: Exibir resultado
        self._exibir_resultado(resultado)
        
        logger.info("Integração concluída com sucesso")
        return resultado

    def _resultado_vazio(self, mensagem: str) -> Dict:
        """Retorna resultado vazio."""
        print("\n" + "=" * 60)
        print("PROCESSAMENTO CONCLUÍDO (SEM DADOS)")
        print("=" * 60)
        return {
            "sucesso": True,
            "registros": 0,
            "com_operadora": 0,
            "sem_operadora": 0,
            "mensagem": mensagem,
            "arquivos_gerados": []
        }

    def _resultado_erro(self, mensagem: str) -> Dict:
        """Retorna resultado de erro."""
        print("\n" + "=" * 60)
        print("✗ ERRO NO PROCESSAMENTO")
        print("=" * 60)
        return {
            "sucesso": False,
            "registros": 0,
            "com_operadora": 0,
            "sem_operadora": 0,
            "erro": mensagem,
            "arquivos_gerados": []
        }

    def _exibir_resultado(self, resultado: Dict) -> None:
        """Exibe resultado do processamento de forma clara."""
        print("\n" + "=" * 60)
        
        if resultado["sucesso"]:
            print("✓ CONSOLIDADOS GERADOS COM SUCESSO")
            print(f"\n  Estatísticas:")
            print(f"  • Total de registros: {resultado['total_registros']:,}")
            print(f"  • Com operadora: {resultado['com_operadora']:,} ({self._percentual(resultado['com_operadora'], resultado['total_registros'])}%)")
            print(f"  • Sem operadora (N/L): {resultado['sem_operadora']:,} ({self._percentual(resultado['sem_operadora'], resultado['total_registros'])}%)")
            
            if resultado.get('registros_com_deducoes'):
                print(f"\n  Arquivos consolidados:")
                print(f"  • Sinistros com deduções: {resultado['registros_com_deducoes']:,} registros")
                print(f"  • Sinistros sem deduções (agregado): {resultado['registros_sem_deducoes']:,} registros")
            
            if resultado.get('arquivos_gerados'):
                print(f"\n  Arquivo gerado:")
                for arquivo in resultado['arquivos_gerados']:
                    print(f"  • {os.path.basename(arquivo)}")
        else:
            print("✗ ERRO AO GERAR CONSOLIDADOS")
            print(f"  {resultado.get('erro', 'Erro desconhecido')}")
        
        print("=" * 60)

    def _percentual(self, parte: int, total: int) -> str:
        """Calcula percentual formatado."""
        if total == 0:
            return "0.0"
        return f"{(parte / total * 100):.1f}"
