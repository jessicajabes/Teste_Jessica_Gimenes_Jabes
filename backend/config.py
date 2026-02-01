import os
from dotenv import load_dotenv

load_dotenv()

API_BASE_URL = os.getenv('API_BASE_URL', 'https://dadosabertos.ans.gov.br/FTP/PDA/')
DATABASE_URL = os.getenv('DATABASE_URL', 'postgresql://jessica:1234@db:55432/intuitive_care')
DEBUG = os.getenv('DEBUG', 'True') == 'True'

