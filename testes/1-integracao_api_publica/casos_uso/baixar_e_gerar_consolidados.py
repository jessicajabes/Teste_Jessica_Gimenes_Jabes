"""Caso de Uso Principal: Baixar e Gerar Consolidados.

Orquestra todo o fluxo de integração:
1. Buscar trimestres disponíveis na API ANS
2. Baixar arquivos ZIP dos trimestres
3. Extrair arquivos CSV dos ZIPs
4. Gerar CSVs consolidados com JOIN pandas (sem banco de dados)
"""

import os
from typing import Dict

from config import DIRETORIO_DOWNLOADS, DIRETORIO_CONSOLIDADO, DIRETORIO_ZIPS, API_BASE_URL
from casos_uso.buscar_trimestres_disponiveis import BuscarTrimestresDisponiveis
from casos_uso.baixar_arquivos_trimestres import BaixarArquivosTrimestres
from infraestrutura.gerenciador_arquivos import GerenciadorArquivos
from infraestrutura.cliente_api_ans import ClienteAPIANS
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

        print(f"[OK] Encontrados {len(trimestres)} trimestres:")
        for trimestre in trimestres:
            print(f"  - {trimestre}")
        
        # Verificar se trimestres são consecutivos e tentar preencher lacunas
        trimestres = self._verificar_e_preencher_trimestres(trimestres)

        # PASSO 2: Baixar arquivos ZIP
        print(f"\n[2/4] Baixando arquivos de {len(trimestres)} trimestres...")
        baixar_arquivos = BaixarArquivosTrimestres()
        arquivos_baixados = baixar_arquivos.executar(trimestres)
        
        if not arquivos_baixados:
            print("[ERRO] Nenhum arquivo foi baixado")
            logger.error("Falha ao baixar arquivos")
            return self._resultado_erro("Nenhum arquivo foi baixado")

        print(f"[OK] {len(arquivos_baixados)} arquivos baixados com sucesso")
        
        # PASSO 2.5: Baixar operadoras (ativas e canceladas)
        print("\n[2.5/4] Baixando arquivo de operadoras...")
        cliente_api = ClienteAPIANS(API_BASE_URL)
        sucesso_operadoras = cliente_api.baixar_operadoras(DIRETORIO_ZIPS)
        
        if not sucesso_operadoras:
            print("⚠ Aviso: Nenhum arquivo de operadoras foi baixado (continuando com os trimestres)")
            logger.warning("Nenhum arquivo de operadoras foi baixado")
        else:
            print("[OK] Operadoras baixadas com sucesso")
        
        cliente_api.fechar()

        # PASSO 3: Extrair ZIPs
        print("\n[3/4] Extraindo arquivos CSV dos ZIPs...")
        gerenciador_arquivos = GerenciadorArquivos()
        gerenciador_arquivos.extrair_zips(DIRETORIO_ZIPS)
        print("[OK] Arquivos extraidos")

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
        print("[ERRO] ERRO NO PROCESSAMENTO")
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
            print("[OK] CONSOLIDADOS GERADOS COM SUCESSO")
            print(f"\n  Estatísticas:")
            print(f"  - Total de registros: {resultado['total_registros']:,}")
            print(f"  - Com operadora: {resultado['com_operadora']:,} ({self._percentual(resultado['com_operadora'], resultado['total_registros'])}%)")
            print(f"  - Sem operadora (N/L): {resultado['sem_operadora']:,} ({self._percentual(resultado['sem_operadora'], resultado['total_registros'])}%)")
            
            if resultado.get('registros_com_deducoes'):
                print(f"\n  Arquivos consolidados:")
                print(f"  - Sinistros com deducoes: {resultado['registros_com_deducoes']:,} registros")
                print(f"  - Sinistros sem deducoes (agregado): {resultado['registros_sem_deducoes']:,} registros")
            
            if resultado.get('arquivos_gerados'):
                print(f"\n  Arquivo gerado:")
                for arquivo in resultado['arquivos_gerados']:
                    print(f"  - {os.path.basename(arquivo)}")
        else:
            print("[ERRO] ERRO AO GERAR CONSOLIDADOS")
            print(f"  {resultado.get('erro', 'Erro desconhecido')}")
        
        print("=" * 60)

    def _percentual(self, parte: int, total: int) -> str:
        """Calcula percentual formatado."""
        if total == 0:
            return "0.0"
        return f"{(parte / total * 100):.1f}"

    def _normalizar_trimestre(self, trimestre) -> str:
        """Normaliza um trimestre para o formato "YYYY/nT".
        
        Aceita formatos:
        - Objeto Trimestre(ano=2025, numero=1)
        - "2025/1T"
        - "2025/1Q"
        - "2025/Q1"
        - "2025_1T"
        - "20251T"
        - "2025 1T"
        
        Args:
            trimestre: Trimestre em qualquer formato (objeto ou string)
            
        Returns:
            Trimestre normalizado no formato "YYYY/nT" ou None se inválido
        """
        import re
        
        if not trimestre:
            return None
        
        # Converter objeto para string se necessário
        trimestre_str = str(trimestre) if not isinstance(trimestre, str) else trimestre
        
        # Remove espaços e normaliza separadores
        t = trimestre_str.strip().replace('_', '/').replace(' ', '/')
        
        # Tenta encontrar padrão YYYY/nT ou YYYY/nQ (número antes de T/Q)
        # Exemplos: 2025/1T, 2025/1Q, 2025-1T
        match = re.search(r'(\d{4})[/\-]?(\d)[/\-]?[TQ]', t, re.IGNORECASE)
        if match:
            ano = match.group(1)
            trim_num = match.group(2)
            if trim_num in '1234':
                return f"{ano}/{trim_num}T"
        
        # Tenta encontrar padrão YYYY/Qn ou YYYY/Tn (letra antes de número)
        # Exemplos: 2025/Q1, 2025/T1
        match = re.search(r'(\d{4})[/\-]?[QT](\d)', t, re.IGNORECASE)
        if match:
            ano = match.group(1)
            trim_num = match.group(2)
            if trim_num in '1234':
                return f"{ano}/{trim_num}T"
        
        # Tenta encontrar padrão direto YYYY#T ou YYYYT# (sem separador)
        # Exemplos: 20251T, 2025T1
        match = re.search(r'(\d{4})(\d)[TQ]?', t, re.IGNORECASE)
        if match:
            ano = match.group(1)
            trim_num = match.group(2)
            if trim_num in '1234':
                return f"{ano}/{trim_num}T"
        
        return None

    def _verificar_e_preencher_trimestres(self, trimestres: list) -> list:
        """Verifica trimestres consecutivos e tenta preencher lacunas automaticamente.
        
        Fluxo:
        1. Verifica se trimestres são consecutivos
        2. Se houver lacunas, tenta baixar TODOS os trimestres do ano faltante
        3. Procura por arquivos com datas do trimestre faltante
        4. Adiciona o trimestre encontrado aos dados a processar
        
        Args:
            trimestres: Lista de trimestres em qualquer formato
            
        Returns:
            Lista de trimestres (pode incluir trimestres preenchidos automaticamente)
        """
        if len(trimestres) < 2:
            return trimestres  # Apenas 1 trimestre, não há como validar
        
        # Normalizar formato dos trimestres para "YYYY/nT"
        trimestres_normalizados = []
        for t in trimestres:
            t_norm = self._normalizar_trimestre(t)
            if t_norm:
                trimestres_normalizados.append(t_norm)
            else:
                logger.warning(f"Nao foi possivel normalizar trimestre: {t}")
        
        if len(trimestres_normalizados) < 2:
            return trimestres  # Se não conseguir normalizar, retorna original
        
        # Converter e ordenar trimestres
        trimestres_parseados = []
        for t in trimestres_normalizados:
            try:
                ano, trimestre = t.split('/')
                trimestre_num = int(trimestre[0])
                trimestres_parseados.append((int(ano), trimestre_num, t))
            except (ValueError, IndexError) as e:
                logger.warning(f"Erro ao processar trimestre normalizado {t}: {e}")
                continue
        
        if not trimestres_parseados:
            return trimestres
        
        trimestres_parseados.sort(key=lambda x: (x[0], x[1]))
        
        # Verificar consecutividade e identificar lacunas
        trimestres_faltando = {}  # {ano: [trims faltando]}
        
        for i in range(len(trimestres_parseados) - 1):
            ano_atual, trim_atual, _ = trimestres_parseados[i]
            ano_proximo, trim_proximo, _ = trimestres_parseados[i + 1]
            
            if trim_atual == 4:
                ano_esperado, trim_esperado = ano_atual + 1, 1
            else:
                ano_esperado, trim_esperado = ano_atual, trim_atual + 1
            
            if (ano_proximo, trim_proximo) != (ano_esperado, trim_esperado):
                # Encontrar trimestres faltando
                ano_falta, trim_falta = ano_esperado, trim_esperado
                while (ano_falta, trim_falta) != (ano_proximo, trim_proximo):
                    if ano_falta not in trimestres_faltando:
                        trimestres_faltando[ano_falta] = []
                    trimestres_faltando[ano_falta].append(trim_falta)
                    
                    if trim_falta == 4:
                        ano_falta += 1
                        trim_falta = 1
                    else:
                        trim_falta += 1
        
        # Se houver lacunas, tentar preenchê-las
        if trimestres_faltando:
            print(f"\n⚠ Trimestres não são consecutivos!")
            print(f"  Faltando: {', '.join([f'{ano}/{t}T' for ano in sorted(trimestres_faltando.keys()) for t in sorted(trimestres_faltando[ano])])}")
            print(f"\n  Tentando preencher lacunas automaticamente...")
            
            trimestres = self._tentar_preencher_lacunas(trimestres, trimestres_faltando)
        else:
            print("\n[OK] Trimestres sao consecutivos")
            logger.info("Trimestres sao consecutivos")
        
        return trimestres

    def _tentar_preencher_lacunas(self, trimestres: list, trimestres_faltando: dict) -> list:
        """Tenta preencher lacunas baixando trimestres do ano faltante.
        
        Fluxo:
        1. Para cada ano com lacunas, baixa TODOS os trimestres
        2. Procura por arquivos com datas correspondentes ao trimestre faltante
        3. Adiciona trimestre encontrado à lista
        4. Se não encontrar, exibe aviso mas continua processamento
        
        Args:
            trimestres: Lista original de trimestres
            trimestres_faltando: Dict com {ano: [trims faltando]}
            
        Returns:
            Lista de trimestres atualizada
        """
        from datetime import datetime
        
        for ano in sorted(trimestres_faltando.keys()):
            trims_faltando = sorted(trimestres_faltando[ano])
            
            print(f"\n  • Buscando dados de {ano}...")
            
            # Tentar baixar TODOS os trimestres do ano faltante
            buscar_trimestres = BuscarTrimestresDisponiveis()
            
            # Simular busca por trimestres do ano específico
            # (Nota: BuscarTrimestresDisponiveis busca últimos 3, então é um fallback)
            try:
                # Baixar dados do ano inteiro
                trimestres_ano = [f"{ano}/1T", f"{ano}/2T", f"{ano}/3T", f"{ano}/4T"]
                
                baixar_arquivos = BaixarArquivosTrimestres()
                print(f"    Baixando dados de {ano}...")
                arquivos_baixados = baixar_arquivos.executar(trimestres_ano)
                
                if not arquivos_baixados:
                    print(f"    ⚠ Não foi possível baixar dados de {ano}")
                    logger.warning(f"Não foi possível baixar dados de {ano}")
                    continue
                
                # Procurar por arquivos com datas do trimestre faltante
                from infraestrutura.gerenciador_arquivos import GerenciadorArquivos
                gerenciador = GerenciadorArquivos()
                
                trimestres_encontrados = self._procurar_trimestres_por_data(
                    trims_faltando, ano
                )
                
                if trimestres_encontrados:
                    for trim in trimestres_encontrados:
                        if trim not in trimestres:
                            trimestres.append(trim)
                            print(f"    [OK] Adicionado: {trim} (encontrado por data)")
                            logger.info(f"Trimestre {trim} adicionado (encontrado por data)")
                else:
                    print(f"    [AVISO] Nenhum arquivo com datas de {ano} encontrado")
                    logger.warning(f"Nenhum arquivo com datas de {ano} encontrado para trimestres: {trims_faltando}")
            
            except Exception as e:
                print(f"    [ERRO] Erro ao processar {ano}: {str(e)}")
                logger.error(f"Erro ao preencher lacuna de {ano}: {str(e)}")
        
        return trimestres

    def _procurar_trimestres_por_data(self, trims_faltando: list, ano: int) -> list:
        """Procura por arquivos com datas do trimestre esperado.
        
        Analisa arquivos CSV no diretório de downloads e verifica
        se a coluna DATA contém datas do trimestre faltante.
        
        Args:
            trims_faltando: Lista de trimestres faltando [1, 2, 3, ou 4]
            ano: Ano para buscar
            
        Returns:
            Lista de trimestres encontrados no formato "YYYY/nT"
        """
        import pandas as pd
        
        trimestres_encontrados = []
        
        # Mapa de meses por trimestre
        meses_por_trimestre = {
            1: [1, 2, 3],      # 1T: Jan, Fev, Mar
            2: [4, 5, 6],      # 2T: Abr, Mai, Jun
            3: [7, 8, 9],      # 3T: Jul, Ago, Set
            4: [10, 11, 12]    # 4T: Out, Nov, Dez
        }
        
        # Procurar em arquivos extraídos
        from config import DIRETORIO_DOWNLOADS, DIRETORIO_ZIPS
        import glob
        
        # Procurar em diferentes locais possíveis
        padroes = [
            os.path.join(DIRETORIO_DOWNLOADS, "**", "*.csv"),
            os.path.join(DIRETORIO_DOWNLOADS, DIRETORIO_ZIPS, "**", "*.csv"),
        ]
        
        for padrao in padroes:
            for arquivo_csv in glob.glob(padrao, recursive=True):
                try:
                    # Ler apenas as primeiras linhas por performance
                    df = pd.read_csv(
                        arquivo_csv, 
                        sep=';',
                        encoding='utf-8-sig',
                        nrows=1000,
                        usecols=['DATA'] if 'DATA' in pd.read_csv(arquivo_csv, sep=';', nrows=1, encoding='utf-8-sig').columns else None
                    )
                    
                    if 'DATA' not in df.columns:
                        continue
                    
                    # Converter datas e extrair mês/ano
                    df['DATA'] = pd.to_datetime(df['DATA'], format='%d/%m/%Y', errors='coerce')
                    df['MES'] = df['DATA'].dt.month
                    df['ANO'] = df['DATA'].dt.year
                    
                    # Procurar por datas do trimestre faltante no ano especificado
                    for trim in trims_faltando:
                        meses = meses_por_trimestre[trim]
                        
                        # Verificar se há datas deste trimestre neste ano
                        tem_dados = df[(df['ANO'] == ano) & (df['MES'].isin(meses))].shape[0] > 0
                        
                        if tem_dados:
                            trimestre_str = f"{ano}/{trim}T"
                            if trimestre_str not in trimestres_encontrados:
                                trimestres_encontrados.append(trimestre_str)
                                logger.debug(f"Trimestre {trimestre_str} encontrado em: {arquivo_csv}")
                
                except Exception as e:
                    # Silenciosamente ignorar erros de leitura em arquivos específicos
                    logger.debug(f"Erro ao ler {arquivo_csv}: {str(e)}")
                    continue
        
        return trimestres_encontrados

    def _verificar_trimestres_consecutivos(self, trimestres: list) -> None:
        """Verifica se os trimestres são consecutivos.
        
        Valida se os trimestres estão em sequência sem lacunas.
        Exemplo válido: 2024/4T, 2025/1T, 2025/2T
        Exemplo inválido: 2024/4T, 2025/2T, 2025/3T (falta 2025/1T)
        
        Args:
            trimestres: Lista de trimestres no formato "YYYY/nT"
        """
        if len(trimestres) < 2:
            return  # Apenas 1 trimestre, não há como validar consecutividade
        
        # Converter e ordenar trimestres
        trimestres_parseados = []
        for t in trimestres:
            try:
                ano, trimestre = t.split('/')
                trimestre_num = int(trimestre[0])  # Remove o 'T'
                trimestres_parseados.append((int(ano), trimestre_num, t))
            except (ValueError, IndexError):
                logger.warning(f"Formato de trimestre inválido: {t}")
                return
        
        # Ordenar por ano e trimestre
        trimestres_parseados.sort(key=lambda x: (x[0], x[1]))
        
        # Verificar consecutividade
        trimestres_faltando = []
        for i in range(len(trimestres_parseados) - 1):
            ano_atual, trim_atual, _ = trimestres_parseados[i]
            ano_proximo, trim_proximo, _ = trimestres_parseados[i + 1]
            
            # Próximo trimestre esperado
            if trim_atual == 4:
                ano_esperado, trim_esperado = ano_atual + 1, 1
            else:
                ano_esperado, trim_esperado = ano_atual, trim_atual + 1
            
            # Verificar se há lacuna
            if (ano_proximo, trim_proximo) != (ano_esperado, trim_esperado):
                # Encontrar trimestres faltando
                ano_falta, trim_falta = ano_esperado, trim_esperado
                while (ano_falta, trim_falta) != (ano_proximo, trim_proximo):
                    trimestres_faltando.append(f"{ano_falta}/{trim_falta}T")
                    if trim_falta == 4:
                        ano_falta += 1
                        trim_falta = 1
                    else:
                        trim_falta += 1
        
        # Exibir aviso se houver lacunas
        if trimestres_faltando:
            mensagem = f"[AVISO] Trimestres nao sao consecutivos! Faltando: {', '.join(trimestres_faltando)}"
            print(f"\n{mensagem}")
            logger.warning(mensagem)
        else:
            print("\n[OK] Trimestres sao consecutivos")
            logger.info("Trimestres sao consecutivos")
