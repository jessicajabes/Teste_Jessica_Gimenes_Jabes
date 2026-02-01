"""Caso de Uso: Gerar Arquivos Consolidados.

Orquestra todo o fluxo de integração de dados da API ANS:
1. Buscar trimestres disponíveis
2. Baixar arquivos dos trimestres
3. Conectar ao banco de dados
4. Carregar operadoras
5. Limpar e resetar dados
6. Extrair ZIPs
7. Processar arquivos
8. Gerar CSVs consolidados
9. Exibir resumo
"""

import os
from typing import Dict

from config import DIRETORIO_DOWNLOADS, DIRETORIO_CONSOLIDADO
from casos_uso.buscar_trimestres import BuscarUltimosTrimestres
from casos_uso.baixar_arquivos import BaixarArquivosTrimestres
from casos_uso.carregar_dados_banco import CarregarDadosBanco
from infraestrutura.gerenciador_checkpoint import GerenciadorCheckpoint
from infraestrutura.repositorio_arquivo_local import RepositorioArquivoLocal
from infraestrutura.logger import get_logger

logger = get_logger("GerarArquivosConsolidados")


class GerarArquivosConsolidados:
    """Caso de uso principal: orquestra todo o fluxo de integração."""
    
    def __init__(self):
        self.gerenciador_checkpoint = GerenciadorCheckpoint()
    
    def executar(self) -> Dict:
        """Executa todas as etapas da integração de forma explícita.
        
        Returns:
            Dict com resultado do processamento
        """
        logger.info("Iniciando Integração de Dados da API Pública ANS")

        print("=" * 60)
        print("SISTEMA DE CONSOLIDAÇÃO DE DADOS - ANS")
        print("=" * 60)

        self.gerenciador_checkpoint.exibir_status()

        # 1. Buscar trimestres
        buscar_trimestres = BuscarUltimosTrimestres()
        trimestres = buscar_trimestres.executar()
        if not trimestres:
            print("Nenhum trimestre encontrado")
            logger.info("Processamento de Integração concluído com sucesso")
            print("\n" + "=" * 60)
            print("PROCESSAMENTO CONCLUÍDO")
            print("=" * 60)
            return {
                "sucesso": True,
                "registros": 0,
                "erros": 0,
                "mensagem": "Nenhum trimestre encontrado"
            }

        print("\nTrimestres selecionados:")
        for trimestre in trimestres:
            print(f"  - {trimestre}")

        # 2. Baixar arquivos
        print("\n2. Baixando arquivos...")
        baixar_arquivos = BaixarArquivosTrimestres()
        arquivos = baixar_arquivos.executar(trimestres)
        print(f"\nTotal de arquivos baixados: {len(arquivos)}")

        # 3. Processar dados e carregar no banco
        if not arquivos:
            return {
                "sucesso": False,
                "registros": 0,
                "erros": 0,
                "mensagem": "Nenhum arquivo foi baixado"
            }

        print("\n3. Carregando dados e gerando CSV...")
        carregar_dados_banco = CarregarDadosBanco()

        # 3.1 Conectar ao banco de dados
        print("\n  Processando dados e inserindo no banco de dados...")
        if not carregar_dados_banco.conectar_banco():
            print("Erro: Não foi possível continuar")
            return {
                "sucesso": False,
                "registros": 0,
                "erros": 0,
                "mensagem": "Falha na conexão com banco de dados"
            }
        # 3.2 Limpar tabela e carregar operadoras        
        carregar_dados_banco.limpar_tabela_operadoras()
        carregar_dados_banco.carregar_operadoras()

        # # 3.3 Limpar tabela de demonstrações contábeis
        # carregar_dados_banco.limpar_tabela_demonstracoes()

        # # 3.4 Resetar checkpoint
        # carregar_dados_banco.resetar_checkpoint()

        # # 3.5 Extrair ZIPs
        # repo_arquivo = RepositorioArquivoLocal()
        # repo_arquivo.extrair_zips(DIRETORIO_DOWNLOADS)

        # # 3.6 Obter arquivos de trimestres baixados
        # arquivos_filtrados = carregar_dados_banco.obter_arquivos_filtrados(DIRETORIO_DOWNLOADS)

        # # 3.7 Processar arquivos - importa dados dos csv para o banco de dados
        # total_registros, total_erros, valor_inicial = carregar_dados_banco.processar_arquivos(
        #     arquivos=arquivos_filtrados,
        #     valor_total_inicial=0.0,
        # )
        total_registros, total_erros, valor_inicial = 0, 0, 0
        # 3.8 Gerar consolidados - busca os dados do banco de dados com join e gera os arquivos consolidados
        diretorio_consolidados = os.path.join(DIRETORIO_DOWNLOADS, DIRETORIO_CONSOLIDADO)
        valor_final = carregar_dados_banco.gerar_consolidados(diretorio_consolidados)

        # 3.9 Finalizar e desconectar
        # carregar_dados_banco.gerenciador_checkpoint.marcar_processamento_completo(
        #     total_registros,
        #     total_erros
        # )
        # carregar_dados_banco.repo_banco.desconectar()

        # 3.10 Exibir resumo
        CarregarDadosBanco.exibir_resumo(
            diretorio_downloads=DIRETORIO_DOWNLOADS,
            total_registros=total_registros,
            total_erros=total_erros,
            valor_total_inicial=valor_inicial,
            valor_total_final=valor_final,
        )

        logger.info(
            f"Processamento concluído - {total_registros} registros carregados, "
            f"{total_erros} erros encontrados"
        )

        logger.info("Processamento de Integração concluído com sucesso")
        print("\n" + "=" * 60)
        print("PROCESSAMENTO CONCLUÍDO")
        print("=" * 60)

        return {
            "sucesso": True,
            "registros": total_registros,
            "erros": total_erros,
            "valor_inicial": valor_inicial,
            "valor_final": valor_final,
            "mensagem": "Processamento concluído com sucesso"
        }
