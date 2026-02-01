"""
Configurações - Transformação e Validação
"""

import os
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.getenv('DATABASE_URL', 'postgresql://jessica:1234@localhost:55432/intuitive_care')
DEBUG = os.getenv('DEBUG', 'True') == 'True'

# Diretórios padrão (sincronizados via volume Docker)
DIRETORIO_INTEGRACAO = os.getenv('DIRETORIO_INTEGRACAO', '/app/downloads')
DIRETORIO_CONSOLIDADOS = os.path.join(DIRETORIO_INTEGRACAO, '1-trimestres_consolidados')
DIRETORIO_TRANSFORMACAO = os.path.join(DIRETORIO_INTEGRACAO, '2-validado_enriquecio')
