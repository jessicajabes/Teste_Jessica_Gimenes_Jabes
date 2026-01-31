import logging
import os

# Configurar logging PRIMEIRO, antes de qualquer outro import
log_dir = '/app/downloads/Integracao/logs'
try:
    os.makedirs(log_dir, exist_ok=True)
except Exception as e:
    print(f"Aviso: Não foi possível criar diretório de logs: {e}")
    # Tentar criar com permissões diferentes
    try:
        os.makedirs(log_dir, mode=0o777, exist_ok=True)
    except:
        # Se ainda assim falhar, usar /tmp como fallback
        log_dir = '/tmp/integracao_logs'
        os.makedirs(log_dir, exist_ok=True)

# Log da sessão (apenas esta execução)
try:
    from datetime import datetime
    session_ts = datetime.now().strftime('%Y%m%d_%H%M%S')
    session_log = os.path.join(log_dir, f'sessao_{session_ts}.log')
    os.environ['LOG_SESSAO_ATUAL'] = session_log
except Exception:
    session_log = None

handlers = [logging.StreamHandler()]

# Tentar adicionar file handler
try:
    log_file = os.path.join(log_dir, 'aplicacao.log')
    handlers.append(logging.FileHandler(log_file, encoding='utf-8'))
except Exception as e:
    print(f"Aviso: Não foi possível criar handler de arquivo: {e}")

if session_log:
    try:
        handlers.append(logging.FileHandler(session_log, encoding='utf-8'))
    except Exception as e:
        print(f"Aviso: Não foi possível criar handler de log da sessão: {e}")

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)-8s | %(filename)s:%(lineno)d | %(funcName)s() | %(message)s',
    handlers=handlers
)

logger = logging.getLogger('IntegracaoAPI')

# AGORA importar os outros módulos - isso garante que eles usem o mesmo FileHandler
from config import API_BASE_URL, DATABASE_URL, DIRETORIO_DOWNLOADS
from casos_uso.buscar_trimestres import BuscarUltimosTrimestres
from casos_uso.baixar_arquivos import BaixarArquivosTrimestres
from casos_uso.carregar_dados_banco import CarregarDadosBanco
from infraestrutura.repositorio_api_http import RepositorioAPIHTTP
from infraestrutura.repositorio_arquivo_local import RepositorioArquivoLocal
from infraestrutura.repositorio_banco_dados import RepositorioBancoDados
from infraestrutura.gerenciador_checkpoint import GerenciadorCheckpoint

def principal():
    logger.info("Iniciando Integração de Dados da API Pública ANS")
    
    repositorio_api = RepositorioAPIHTTP(API_BASE_URL)
    repositorio_arquivo = RepositorioArquivoLocal()
    repositorio_banco = RepositorioBancoDados(DATABASE_URL)
    gerenciador_checkpoint = GerenciadorCheckpoint()
    
    print("="*60)
    print("SISTEMA DE CONSOLIDAÇÃO DE DADOS - ANS")
    print("="*60)
    
    gerenciador_checkpoint.exibir_status()
    
    print("\n1. Buscando últimos 3 trimestres...")
    buscar_trimestres = BuscarUltimosTrimestres(repositorio_api, quantidade=3)
    trimestres = buscar_trimestres.executar()
    
    if not trimestres:
        print("Nenhum trimestre encontrado")
        return
    
    print(f"\nTrimestres selecionados:")
    for trimestre in trimestres:
        print(f"  - {trimestre}")

    print("\n2. Baixando arquivos...")
    baixar_arquivos = BaixarArquivosTrimestres(repositorio_api)
    arquivos = baixar_arquivos.executar(trimestres)
    
    print(f"\nTotal de arquivos baixados: {len(arquivos)}")
    
    if arquivos:
        print("\n3. Carregando dados e gerando CSV...")
        carregar_banco = CarregarDadosBanco(repositorio_arquivo, repositorio_banco)
        resultado = carregar_banco.executar(trimestres, DIRETORIO_DOWNLOADS)
        
        print(f"\nTotal de registros processados: {resultado.get('registros', 0)}")
    
    repositorio_api.fechar()
    
    logger.info("Processamento de Integração concluído com sucesso")
    
    print("\n" + "="*60)
    print("PROCESSAMENTO CONCLUÍDO")
    print("="*60)

if __name__ == '__main__':
    principal()
