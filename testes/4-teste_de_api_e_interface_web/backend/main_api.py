"""API FastAPI - Teste de API e Interface Web"""
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware

from config import CORS_ORIGINS
from schemas import PaginacaoOperadoras, OperadoraDetalhe, DespesaOperadora, Estatisticas
from repositories import listar_operadoras, obter_operadora_por_cnpj, obter_despesas_por_cnpj
from services import get_estatisticas_cached


app = FastAPI(title="Teste de API e Interface Web")

app.add_middleware(
    CORSMiddleware,
    allow_origins=CORS_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"]
)


@app.get("/api/operadoras", response_model=PaginacaoOperadoras)
def api_listar_operadoras(
    page: int = Query(1, ge=1),
    limit: int = Query(10, ge=1, le=100),
    q: str | None = None,
):
    resultado = listar_operadoras(page=page, limit=limit, busca=q)
    return {
        "data": resultado["data"],
        "total": resultado["total"],
        "page": page,
        "limit": limit,
    }


@app.get("/api/operadoras/{cnpj}", response_model=OperadoraDetalhe)
def api_obter_operadora(cnpj: str):
    operadora = obter_operadora_por_cnpj(cnpj)
    if not operadora:
        raise HTTPException(status_code=404, detail="Operadora não encontrada")
    return operadora


@app.get("/api/operadoras/{cnpj}/despesas", response_model=list[DespesaOperadora])
def api_obter_despesas(cnpj: str):
    despesas = obter_despesas_por_cnpj(cnpj)
    if not despesas:
        raise HTTPException(status_code=404, detail="Despesas não encontradas para a operadora")
    return despesas


@app.get("/api/estatisticas", response_model=Estatisticas)
def api_estatisticas():
    return get_estatisticas_cached()
