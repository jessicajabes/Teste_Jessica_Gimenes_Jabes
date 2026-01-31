import os
from dotenv import load_dotenv

load_dotenv()

API_BASE_URL = os.getenv('API_BASE_URL', 'https://dadosabertos.ans.gov.br/FTP/PDA/')
DATABASE_URL = os.getenv('DATABASE_URL', 'postgresql://jessica:1234@db:55432/intuitive_care')
DEBUG = os.getenv('DEBUG', 'True') == 'True'

# Configurações de cache para operadoras
FORCAR_ATUALIZACAO_OPERADORAS = os.getenv('FORCAR_ATUALIZACAO_OPERADORAS', 'False') == 'True'
DIAS_VALIDADE_CACHE_OPERADORAS = int(os.getenv('DIAS_VALIDADE_CACHE_OPERADORAS', '30'))
