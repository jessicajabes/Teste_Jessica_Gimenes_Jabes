"""
Gerenciamento de conexão com o banco de dados
"""
import psycopg2
from psycopg2.extras import RealDictCursor
from contextlib import contextmanager
from typing import Generator
from app.config import get_settings

settings = get_settings()


@contextmanager
def get_db_connection() -> Generator:
    """
    Context manager para conexão com o banco de dados
    Garante que a conexão será fechada após o uso
    """
    conn = None
    try:
        conn = psycopg2.connect(
            settings.database_url,
            cursor_factory=RealDictCursor
        )
        yield conn
    except Exception as e:
        if conn:
            conn.rollback()
        raise e
    finally:
        if conn:
            conn.close()


def test_connection() -> bool:
    """Testa a conexão com o banco de dados"""
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
                return True
    except Exception as e:
        print(f"Erro ao conectar ao banco: {e}")
        return False
