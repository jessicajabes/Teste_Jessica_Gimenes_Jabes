"""Configurações do módulo 4 - API e Interface Web"""
import os
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://jessica:1234@localhost:55432/intuitive_care",
)

API_HOST = os.getenv("API_HOST", "0.0.0.0")
API_PORT = int(os.getenv("API_PORT", "8000"))

# Cache de estatísticas (segundos)
STATS_CACHE_TTL = int(os.getenv("STATS_CACHE_TTL", "600"))

# CORS
CORS_ORIGINS = os.getenv("CORS_ORIGINS", "http://localhost:5173").split(",")
