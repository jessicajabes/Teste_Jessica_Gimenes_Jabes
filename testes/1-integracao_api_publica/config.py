import os
from dotenv import load_dotenv

load_dotenv()

API_BASE_URL = os.getenv('API_BASE_URL', 'https://dadosabertos.ans.gov.br/FTP/PDA/')
DATABASE_URL = os.getenv('DATABASE_URL', 'postgresql://jessica:1234@db:55432/intuitive_care')
DEBUG = os.getenv('DEBUG', 'True') == 'True'
BATCH_SIZE = int(os.getenv('BATCH_SIZE', '1000'))

# Diretórios de trabalho (sincronizados via volume Docker)
DIRETORIO_DOWNLOADS = os.getenv('DIRETORIO_DOWNLOADS', '/app/downloads')
DIRETORIO_ZIPS = os.path.join(DIRETORIO_DOWNLOADS, 'arquivos_trimestres')
DIRETORIO_EXTRAIDO = os.path.join(DIRETORIO_DOWNLOADS, 'arquivos_zip_trimestre_extraido')
DIRETORIO_CONSOLIDADO = os.path.join(DIRETORIO_DOWNLOADS, '1-trimestres_consolidados')
DIRETORIO_ERROS = os.path.join(DIRETORIO_DOWNLOADS, 'erros')
DIRETORIO_CHECKPOINTS = os.path.join(DIRETORIO_DOWNLOADS, 'checkpoints')
DIRETORIO_OPERADORAS = os.path.join(DIRETORIO_DOWNLOADS, 'operadoras')
