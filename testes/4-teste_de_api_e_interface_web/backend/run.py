"""Script para iniciar o servidor FastAPI"""
import uvicorn
from config import API_HOST, API_PORT

if __name__ == "__main__":
    uvicorn.run(
        "app:app",
        host=API_HOST,
        port=API_PORT,
        reload=True,
        log_level="info"
    )
