import os
import re
import pandas as pd
import zipfile
from typing import List, Dict, Tuple, Optional
from domain.repositorios import RepositorioArquivo
from infraestrutura.repositorio_banco_dados import RepositorioBancoDados
from infraestrutura.gerenciador_checkpoint import GerenciadorCheckpoint
from infraestrutura.processador_em_lotes import ProcessadorEmLotes
from infraestrutura.logger import get_logger
from casos_uso.carregar_operadoras import CarregarOperadoras

logger = get_logger('CarregarDadosBanco')

class CarregarDadosBanco:
    PALAVRAS_CHAVE = ["Despesas com Eventos/Sinistros"]
    ENCODINGS = ['utf-8', 'latin-1', 'iso-8859-1', 'cp1252']  # Ordem de prioridade
    
    def __init__(self, repo_arquivo: RepositorioArquivo, repo_banco: RepositorioBancoDados):
        self.repo_arquivo = repo_arquivo
        self.repo_banco = repo_banco
        self.gerenciador_checkpoint = GerenciadorCheckpoint()
        self.processador_lotes = ProcessadorEmLotes(tamanho_lote=100)
        self.carregador_operadoras = CarregarOperadoras()
    
    @staticmethod
    def _ler_arquivo_com_encoding(caminho: str, sep: str = ';', **kwargs) -> Optional[pd.DataFrame]:
        """Tenta ler arquivo com múltiplos encodings"""
        for encoding in CarregarDadosBanco.ENCODINGS:
            try:
                df = pd.read_csv(caminho, sep=sep, encoding=encoding, quotechar='"', on_bad_lines='skip', **kwargs)
                # Se conseguiu ler, limpar caracteres problemáticos
                for col in df.columns:
                    if df[col].dtype == 'object':
                        # Corrigir encoding quebrado (ex: SaÃºde -> Saúde)
                        df[col] = df[col].apply(lambda x: x.encode('latin-1').decode('utf-8', errors='ignore') if isinstance(x, str) else x)
                logger.debug(f"Arquivo {os.path.basename(caminho)} lido com sucesso usando encoding: {encoding}")
                return df
            except Exception as e:
                logger.debug(f"Falha ao ler com {encoding}: {str(e)[:50]}")
                continue
        
        logger.error(f"Não foi possível ler arquivo com nenhum encoding: {caminho}")
        return None
    
    def executar(self, trimestres: List, diretorio_downloads: str) -> Dict:
        print("\nProcessando dados e inserindo no banco de dados...")
        
        # Exibir status do checkpoint no início
        self.gerenciador_checkpoint.exibir_status()
        
        if not self.repo_banco.conectar():
            print("Erro: Não foi possível conectar ao banco de dados")
            return {"registros": 0, "erros": 0, "valor_inicial": 0.0, "valor_final": 0.0}
        
        checkpoint = self.gerenciador_checkpoint.obter_checkpoint()
        
        # Variável para acumular valor total inicial durante o processamento
        valor_total_inicial = 0.0
        
        # Carregar e inserir tabelas de operadoras no banco
        print("\n📋 Carregando tabelas de operadoras...")
        resultado_operadoras = self.carregador_operadoras.executar()
        if resultado_operadoras['ativas'] or resultado_operadoras['canceladas']:
            print("   Inserindo operadoras no banco de dados...")
            self.repo_banco.limpar_tabela_operadoras()
            
            # Inserir operadoras ativas
            if resultado_operadoras['ativas']:
                import pandas as pd
                df_ativas = pd.read_csv(
                    self.carregador_operadoras.arquivo_ativas, 
                    sep=';', 
                    encoding='utf-8'
                )
                df_ativas['STATUS'] = 'ATIVA'
                dados_ativas = df_ativas.to_dict('records')
                total_ativas = self.repo_banco.inserir_operadoras(dados_ativas)
                print(f"     {total_ativas} operadoras ativas inseridas")
            
            # Inserir operadoras canceladas
            if resultado_operadoras['canceladas']:
                import pandas as pd
                df_canceladas = pd.read_csv(
                    self.carregador_operadoras.arquivo_canceladas, 
                    sep=';', 
                    encoding='utf-8'
                )
                df_canceladas['STATUS'] = 'CANCELADA'
                dados_canceladas = df_canceladas.to_dict('records')
                total_canceladas = self.repo_banco.inserir_operadoras(dados_canceladas)
                print(f"     {total_canceladas} operadoras canceladas inseridas")
        else:
            logger.warning("Nenhuma tabela de operadora foi carregada com sucesso")
            print("    Aviso: Operadoras não foram carregadas. Continuando sem enriquecimento...")
        
        # Limpar tabela de demonstrações SEMPRE antes de processar (como operadoras)
        print("\n🗑️  Limpando tabela de demonstrações contábeis...")
        self.repo_banco.limpar_tabela()
        print("   Tabela limpa com sucesso!")
        
        # Resetar checkpoint para garantir que vai processar todos os arquivos
        print("\n🔄 Resetando checkpoint para processar todos os trimestres...")
        self.gerenciador_checkpoint.resetar_checkpoint()
        checkpoint = self.gerenciador_checkpoint.obter_checkpoint()
        
        # Extrair ZIPs se necessário
        self.repo_arquivo.extrair_zips(diretorio_downloads)
        
        arquivos = self._obter_arquivos_filtrados(diretorio_downloads)
        
        total_registros = 0
        total_erros = 0
        
        for tipo, caminhos in arquivos.items():
            for caminho in caminhos:
                nome_arquivo = os.path.basename(caminho)
                
                print(f"\n  Processando: {nome_arquivo}")
                
                ano, trimestre = self._extrair_ano_trimestre_arquivo(caminho)
                if ano is None or trimestre is None:
                    print("    Erro: não foi possível identificar Ano/Trimestre para este arquivo, pulando...")
                    continue
                print(f"    Ano: {ano}, Trimestre: {trimestre}")
                dados = self._extrair_dados_arquivo(caminho, ano, trimestre)
                
                print(f"    Registros extraídos: {len(dados) if dados else 0}")
                
                if dados:
                    # Calcular valor inicial deste arquivo antes de inserir
                    valor_arquivo = self._calcular_valor_arquivo(dados)
                    valor_total_inicial += valor_arquivo
                    print(f"    Valor do arquivo (Final - Inicial): R$ {valor_arquivo:,.2f}".replace(',', '#').replace('.', ',').replace('#', '.'))
                    
                    resultado = self.processador_lotes.processar_em_lotes(
                        registros=dados,
                        funcao_inserir=self.repo_banco.inserir_demonstracoes,
                        gerenciador_checkpoint=self.gerenciador_checkpoint,
                        arquivo_atual=nome_arquivo,
                        registro_inicial=checkpoint.get("registro_atual", 0)
                    )
                    
                    total_registros += resultado["registros_processados"]
                    total_erros += resultado["registros_com_erro"]
                    
                    # Registrar o trimestre como processado com sucesso
                    self.gerenciador_checkpoint.marcar_trimestre_processado(ano, trimestre)
                
                self.gerenciador_checkpoint.marcar_arquivo_completo(nome_arquivo)
                checkpoint["arquivo_atual"] = nome_arquivo
                checkpoint["registro_atual"] = 0
        
        # Gerar CSV consolidado fazendo JOIN no banco de dados
        print("\n Gerando CSVs consolidados com JOIN no banco...")
        diretorio_consolidados = os.path.join(diretorio_downloads, 'consolidados')
        log_sessao = os.getenv('LOG_SESSAO_ATUAL')
        sucesso = self.repo_banco.gerar_csv_consolidado_com_join(diretorio_consolidados, arquivo_log_sessao=log_sessao)
        
        if sucesso:
            print("   CSVs consolidados gerados com sucesso!")
        else:
            print("   Erro ao gerar CSVs consolidados (tentando calcular valor mesmo assim)")
        
        # Calcular valor total final SEMPRE (do banco de dados, não do CSV)
        # Isso garante que temos o valor mesmo se o CSV falhar
        valor_total_final = self.repo_banco.calcular_valor_total_csv(diretorio_consolidados)
        
        self.gerenciador_checkpoint.marcar_processamento_completo(total_registros, total_erros)
        
        self.repo_banco.desconectar()
        logger.info(f"Processamento concluído - {total_registros} registros carregados, {total_erros} erros encontrados")
        
        print(f"\n{'='*60}")
        print(f"PROCESSAMENTO CONCLUÍDO")
        print(f"{'='*60}")
        print(f"Total de registros carregados: {total_registros}")
        print(f"Total de erros: {total_erros}")
        print(f"Arquivos gerados em: {os.path.join(diretorio_downloads, 'consolidados')}/")
        print(f"  • consolidado_despesas_sinistros.csv")
        print(f"  • consolidado_todas_despesas.csv")
        print(f"  • consolidado_despesas.zip")
        
        # Exibir comparativo de valores
        print(f"\n📊 COMPARATIVO DE VALORES")
        print(f"{'='*60}")
        print(f"Valor Total Inicial (arquivos brutos): R$ {valor_total_inicial:,.2f}".replace(',', '#').replace('.', ',').replace('#', '.'))
        print(f"Valor Total Final (CSV gerado):        R$ {valor_total_final:,.2f}".replace(',', '#').replace('.', ',').replace('#', '.'))
        diferenca = valor_total_inicial - valor_total_final
        percentual = (diferenca / valor_total_inicial * 100) if valor_total_inicial != 0 else 0
        print(f"Diferença:                              R$ {diferenca:,.2f}".replace(',', '#').replace('.', ',').replace('#', '.'))
        print(f"Percentual:                             {percentual:.2f}%")
        print(f"{'='*60}\n")
        
        logger.info(f"Comparativo: Inicial={valor_total_inicial}, Final={valor_total_final}, Diferença={diferenca}, Percentual={percentual}%")
        
        return {
            "registros": total_registros,
            "erros": total_erros,
            "valor_inicial": valor_total_inicial,
            "valor_final": valor_total_final
        }
    
    def _calcular_valor_arquivo(self, dados: List[Dict]) -> float:
        """Calcula o valor total (VL_SALDO_FINAL - VL_SALDO_INICIAL) de um conjunto de dados já extraídos"""
        try:
            valor_total = 0.0
            for registro in dados:
                try:
                    # Obter valores dos campos
                    vl_final = registro.get('VL_SALDO_FINAL', 0)
                    vl_inicial = registro.get('VL_SALDO_INICIAL', 0)
                    
                    # Converter para numérico se necessário
                    if isinstance(vl_final, str):
                        vl_final = float(vl_final.replace('.', '').replace(',', '.')) if vl_final else 0
                    if isinstance(vl_inicial, str):
                        vl_inicial = float(vl_inicial.replace('.', '').replace(',', '.')) if vl_inicial else 0
                    
                    # Somar diferença
                    valor_total += (float(vl_final or 0) - float(vl_inicial or 0))
                except Exception as e:
                    logger.debug(f"Erro ao calcular valor de registro: {e}")
                    continue
            
            return valor_total
        except Exception as e:
            logger.error(f"Erro ao calcular valor do arquivo: {e}")
            return 0.0
    
    def _obter_arquivos_filtrados(self, diretorio: str) -> Dict:
        return self.repo_arquivo.encontrar_arquivos_dados(diretorio)
    
    def _contem_palavras_chave(self, caminho_arquivo: str) -> bool:
        try:
            if caminho_arquivo.endswith('.csv'):
                df = self._ler_arquivo_com_encoding(caminho_arquivo, sep=';')
            elif caminho_arquivo.endswith('.txt'):
                df = self._ler_arquivo_com_encoding(caminho_arquivo, sep='\t')
            elif caminho_arquivo.endswith('.xlsx'):
                df = pd.read_excel(caminho_arquivo)
            else:
                return False
            
            if df is None:
                return False
            
            conteudo = df.to_string().upper()
            
            for palavra in self.PALAVRAS_CHAVE:
                if palavra.upper() in conteudo:
                    return True
            
            return False
        except Exception as e:
            logger.error(f"Erro ao verificar palavras-chave no arquivo: {e}")
            print(f"      Erro ao verificar palavras-chave: {e}")
            return False
    
    def _extrair_dados_arquivo(self, caminho_arquivo: str, ano: int, trimestre: int) -> List[Dict]:
        try:
            if caminho_arquivo.endswith('.csv'):
                df = self._ler_arquivo_com_encoding(caminho_arquivo, sep=';')
            elif caminho_arquivo.endswith('.txt'):
                df = self._ler_arquivo_com_encoding(caminho_arquivo, sep='\t')
            elif caminho_arquivo.endswith('.xlsx'):
                df = pd.read_excel(caminho_arquivo)
            else:
                return []
            
            if df is None:
                return []
            
            print(f"    Linhas no arquivo: {len(df)}")
            print(f"    Colunas: {list(df.columns)[:5]}...")
            
            # Verificar se o arquivo contém a palavra-chave
            if not self._contem_palavras_chave(caminho_arquivo):
                print(f"    Arquivo não contém '{self.PALAVRAS_CHAVE[0]}', pulando...")
                return []
            
            print(f"    Arquivo contém '{self.PALAVRAS_CHAVE[0]}', processando todos os dados...")
            
            df.columns = df.columns.str.upper().str.strip().str.replace(' ', '_')
            
            dados = []
            registros_rejeitados = 0
            valor_rejeitado = 0.0
            
            # Usar itertuples() em vez de iterrows() para melhor performance (10-100x mais rápido)
            colunas = list(df.columns)
            for idx, linha in enumerate(df.itertuples(index=False, name=None)):
                # Converter tupla para dicionário
                registro = dict(zip(colunas, linha))
                
                saldo_inicial = self._extrair_numero(registro, ['VL_SALDO_INICIAL', 'SALDO_INICIAL'])
                saldo_final = self._extrair_numero(registro, ['VL_SALDO_FINAL', 'SALDO_FINAL'])
                
                registro['TRIMESTRE'] = trimestre
                registro['ANO'] = ano
                
                # Calcular valor do trimestre (despesas/receitas)
                try:
                    vi = float(saldo_inicial or 0)
                    vf = float(saldo_final or 0)
                    registro['VALOR_TRIMESTRE'] = vf - vi
                except:
                    registro['VALOR_TRIMESTRE'] = None
                
                # Validar apenas campos essenciais
                if self._validar_registro(registro):
                    dados.append(registro)
                else:
                    registros_rejeitados += 1
                    # Rastrear valor dos registros rejeitados
                    try:
                        vf = float(saldo_final or 0)
                        vi = float(saldo_inicial or 0)
                        valor_rejeitado += (vf - vi)
                    except:
                        pass
                    
                    if registros_rejeitados <= 5:
                        logger.debug(f"Registro {idx} rejeitado na validação:")
                        logger.debug(f"  REG_ANS={registro.get('REG_ANS')}, CD_CONTA={registro.get('CD_CONTA_CONTABIL')}")
                        logger.debug(f"  SALDO_INICIAL={saldo_inicial}, SALDO_FINAL={saldo_final}")
            
            print(f"    Registros válidos: {len(dados)}")
            print(f"    Registros rejeitados (validação): {registros_rejeitados}")
            if registros_rejeitados > 0:
                logger.warning(f"⚠️  {registros_rejeitados} registros rejeitados na validação (Arquivo: {caminho_arquivo})")
                logger.warning(f"   Valor total dos registros rejeitados: R$ {valor_rejeitado:,.2f}")
            
            return dados
        except Exception as e:
            logger.error(f"Erro ao processar arquivo {caminho_arquivo}: {e}")
            print(f"    Erro ao processar arquivo: {e}")
            return []
    
    @staticmethod
    def _extrair_data(linha) -> str:
        colunas_data = ['DATA', 'DATA_INICIO', 'DATA_TRIMESTRE']
        
        # Suporta tanto Series (com .index) quanto dicionários
        is_dict = isinstance(linha, dict)
        
        for col in colunas_data:
            existe = (col in linha.index) if not is_dict else (col in linha)
            if existe:
                valor = linha[col]
                if pd.notna(valor):
                    valor_str = str(valor).strip()
                    match = re.search(r'(\d{4})-(\d{2})-(\d{2})', valor_str)
                    if match:
                        return valor_str
                    match = re.search(r'(\d{2})/(\d{2})/(\d{4})', valor_str)
                    if match:
                        return f"{match.group(3)}-{match.group(2)}-{match.group(1)}"
        
        return None
    
    @staticmethod
    def _extrair_numero(linha, colunas_possiveis) -> float:
        # Suporta tanto Series (com .index) quanto dicionários
        is_dict = isinstance(linha, dict)
        
        for col in colunas_possiveis:
            existe = (col in linha.index) if not is_dict else (col in linha)
            if existe:
                valor = linha[col]
                if pd.notna(valor):
                    try:
                        return float(valor)
                    except (ValueError, TypeError):
                        pass
        return None
    
    def _extrair_ano_trimestre_arquivo(self, caminho_arquivo: str) -> Tuple[Optional[int], Optional[int]]:
        # Extrai o nome do arquivo (ex: 1T2025.csv)
        nome_arquivo = os.path.basename(caminho_arquivo)
        
        # Procura pelo padrão {trimestre}T{ano} no nome do arquivo
        # Exemplo: 1T2025, 2T2024, 3T2023, etc
        match = re.search(r'(\d+)[tT](\d{4})', nome_arquivo)
        
        ano_arquivo = None
        trimestre_arquivo = None
        if match:
            trimestre_arquivo = int(match.group(1))
            ano_arquivo = int(match.group(2))

        # Tentar extrair pelo conteúdo do arquivo (coluna DATA)
        ano_data, trimestre_data = self._extrair_ano_trimestre_por_data(caminho_arquivo)

        # Se achou no nome, apenas verifica e prioriza a data em caso de divergência
        if ano_arquivo is not None and trimestre_arquivo is not None:
            if ano_data is not None and trimestre_data is not None:
                if ano_data != ano_arquivo or trimestre_data != trimestre_arquivo:
                    logger.warning(
                        f"Divergência detectada no arquivo {os.path.basename(caminho_arquivo)}: "
                        f"nome ({ano_arquivo}T{trimestre_arquivo}) vs data ({ano_data}T{trimestre_data}). "
                        f"Usando data do arquivo."
                    )
                    print(
                        f"    Aviso: Ano/Trimestre do nome ({ano_arquivo} T{trimestre_arquivo}) "
                        f"diverge da data no arquivo ({ano_data} T{trimestre_data}). Usando a data do arquivo."
                    )
                    self.repo_banco.registrar_erro_validacao({
                        'arquivo': os.path.basename(caminho_arquivo),
                        'ano_nome': ano_arquivo,
                        'trimestre_nome': trimestre_arquivo,
                        'ano_data': ano_data,
                        'trimestre_data': trimestre_data,
                        'motivo_erro': 'Divergência entre nome do arquivo e data interna'
                    })
                    return ano_data, trimestre_data
            return ano_arquivo, trimestre_arquivo

        # Se não achou no nome, usa a data do arquivo
        if ano_data is not None and trimestre_data is not None:
            return ano_data, trimestre_data

        # Não conseguiu determinar
        return None, None

    def _extrair_ano_trimestre_por_data(self, caminho_arquivo: str) -> Tuple[Optional[int], Optional[int]]:
        try:
            if caminho_arquivo.endswith('.csv'):
                df = self._ler_arquivo_com_encoding(caminho_arquivo, sep=';')
            elif caminho_arquivo.endswith('.txt'):
                df = self._ler_arquivo_com_encoding(caminho_arquivo, sep='\t')
            elif caminho_arquivo.endswith('.xlsx'):
                df = pd.read_excel(caminho_arquivo)
            else:
                return None, None
            
            if df is None:
                return None, None

            df.columns = df.columns.str.upper().str.strip().str.replace(' ', '_')

            for _, linha in df.iterrows():
                data_str = self._extrair_data(linha)
                if not data_str:
                    continue

                match = re.search(r'(\d{4})-(\d{2})-(\d{2})', data_str)
                if not match:
                    continue

                ano = int(match.group(1))
                mes = int(match.group(2))
                trimestre = (mes - 1) // 3 + 1
                return ano, trimestre

            return None, None
        except Exception as e:
            print(f"    Erro ao extrair Ano/Trimestre pela data: {e}")
            return None, None
    
    @staticmethod
    def _validar_registro(registro: Dict) -> bool:
        return True
