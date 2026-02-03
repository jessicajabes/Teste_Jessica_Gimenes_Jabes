"""Caso de Uso: Carregar Dados no Banco de Dados.

Orquestra o carregamento de dados no banco usando serviços de domínio.
"""

import os
from typing import Dict, Tuple

from config import DATABASE_URL, BATCH_SIZE
from domain.servicos import ProcessadorArquivos, GeradorConsolidados
from infraestrutura.repositorio_arquivo_local import RepositorioArquivoLocal
from infraestrutura.repositorio_csv import RepositorioCsv
from infraestrutura.gerenciador_checkpoint import GerenciadorCheckpoint
from infraestrutura.processador_em_lotes import ProcessadorEmLotes
from infraestrutura.logger import get_logger

logger = get_logger('CarregarDadosBanco')


class CarregarDadosBanco:
    """Orquestra o carregamento completo de dados no banco."""
    
    def __init__(
        self,
        repo_arquivo=None,
        repo_banco=None,
    ):
        self.repo_arquivo = repo_arquivo or RepositorioArquivoLocal()
        self.repo_banco = repo_banco or RepositorioCsv(DATABASE_URL)
        self.gerenciador_checkpoint = GerenciadorCheckpoint()
        self.processador_lotes = ProcessadorEmLotes(tamanho_lote=BATCH_SIZE)
        
        # Injetar serviços de domínio
        self.processador = ProcessadorArquivos()
        self.gerador = GeradorConsolidados()
    
    def conectar_banco(self) -> bool:
        """Estabelece conexão com o banco de dados."""
        if self.repo_banco.conectar():
            return True
        print("Erro: Não foi possível conectar ao banco de dados")
        return False
    
    # Métodos de operadoras removidos - agora processadas diretamente no Python durante a consolidação
    
    def limpar_tabela_demonstracoes(self) -> None:
        """Limpa a tabela de demonstrações contábeis."""
        print("\n  Limpando tabela de demonstrações contábeis...")
        self.repo_banco.limpar_tabela()
        print("   Tabela limpa com sucesso!")
    
    def resetar_checkpoint(self) -> None:
        """Reseta o checkpoint para processar todos os trimestres."""
        print("\n Resetando checkpoint para processar todos os trimestres...")
        self.gerenciador_checkpoint.resetar_checkpoint()
    
    def processar_arquivos(
        self,
        arquivos: Dict,
        valor_total_inicial: float
    ) -> Tuple[int, int, float]:
        """Processa todos os arquivos filtrados.
        
        Orquestra:
        1. Leitura de arquivos com múltiplos encodings
        2. Validação de dados
        3. Inserção em lotes no banco
        4. Gestão de checkpoints
        """
        checkpoint = self.gerenciador_checkpoint.obter_checkpoint()
        total_registros = 0
        total_erros = 0
        
        for _, caminhos in arquivos.items():
            for caminho in caminhos:
                nome_arquivo = os.path.basename(caminho)
                print(f"\n  Carregando arquivo: {nome_arquivo}...", flush=True)
                
                # Usar serviço de domínio para extrair dados
                ano, trimestre = self._extrair_ano_trimestre_arquivo(caminho)
                if ano is None or trimestre is None:
                    print("    Erro: não foi possível identificar Ano/Trimestre para este arquivo, pulando...")
                    continue
                
                dados, valor_arquivo, rejeitados = self.processador.extrair_dados_arquivo(
                    caminho,
                    ano,
                    trimestre
                )
                
                if dados:
                    total_rejeitados = rejeitados if rejeitados is not None else 0
                    print(
                        f"    Arquivo carregado: {len(dados)} registros válidos"
                        f" | {total_rejeitados} rejeitados",
                        flush=True,
                    )
                    valor_total_inicial += valor_arquivo
                    print(
                        f"    Valor do arquivo (Final - Inicial): R$ {valor_arquivo:,.2f}"
                        .replace(',', '#')
                        .replace('.', ',')
                        .replace('#', '.')
                    )
                    
                    # Processar em lotes (SEM checkpoint frequente para máxima performance)
                    resultado = self.processador_lotes.processar_em_lotes(
                        registros=dados,
                        funcao_inserir=self.repo_banco.inserir_demonstracoes,
                        gerenciador_checkpoint=self.gerenciador_checkpoint,
                        arquivo_atual=nome_arquivo,
                        registro_inicial=checkpoint.get("registro_atual", 0),
                        atualizar_checkpoint=False  # Desabilitar checkpoint frequente para performance
                    )
                    
                    total_registros += resultado["registros_processados"]
                    total_erros += resultado["registros_com_erro"]
                    self.gerenciador_checkpoint.marcar_trimestre_processado(ano, trimestre)
                
                self.gerenciador_checkpoint.marcar_arquivo_completo(nome_arquivo)
                checkpoint["arquivo_atual"] = nome_arquivo
                checkpoint["registro_atual"] = 0
        
        return total_registros, total_erros, valor_total_inicial
    
    def gerar_consolidados(self, diretorio_consolidados: str) -> float:
        """Gera CSVs consolidados com JOIN no banco."""
        print("\n Gerando CSVs consolidados com JOIN no banco...")
        log_sessao = os.getenv('LOG_SESSAO_ATUAL')
        sucesso = self.repo_banco.gerar_csv_consolidado_com_join(
            diretorio_consolidados,
            arquivo_log_sessao=log_sessao,
        )
        
        if sucesso:
            print("   CSVs consolidados gerados com sucesso!")
        else:
            print("   Erro ao gerar CSVs consolidados (tentando calcular valor mesmo assim)")
        
        return self.repo_banco.calcular_valor_total_csv(diretorio_consolidados)
    
    def obter_arquivos_filtrados(self, diretorio: str) -> Dict:
        """Obtém arquivos filtrados do diretório."""
        return self.repo_arquivo.encontrar_arquivos_dados(diretorio)
    
    @staticmethod
    def _extrair_ano_trimestre_arquivo(caminho: str) -> Tuple[int, int]:
        """Extrai ano e trimestre do nome do arquivo.
        
        Padrões esperados:
        - 2024T1... ou 2024_T1... (ano primeiro)
        - 1T2024... ou 1T2024.csv (trimestre primeiro)
        """
        import re
        
        # Tenta padrão: ano primeiro (2024T1 ou 2024_T1)
        match = re.search(r'(\d{4})[_-]?T(\d)', caminho, re.IGNORECASE)
        if match:
            return int(match.group(1)), int(match.group(2))
        
        # Tenta padrão: trimestre primeiro (1T2024 ou 1T2024.csv)
        match = re.search(r'(\d)T(\d{4})', caminho, re.IGNORECASE)
        if match:
            return int(match.group(2)), int(match.group(1))  # Retorna (ano, trimestre)
        
        return None, None
    
    @staticmethod
    def exibir_resumo(
        diretorio_downloads: str,
        total_registros: int,
        total_erros: int,
        valor_total_inicial: float,
        valor_total_final: float,
    ) -> None:
        """Exibe resumo do processamento usando serviço de domínio."""
        GeradorConsolidados.exibir_resumo_consolidacao(
            diretorio_downloads,
            total_registros,
            total_erros,
            valor_total_inicial,
            valor_total_final,
        )
