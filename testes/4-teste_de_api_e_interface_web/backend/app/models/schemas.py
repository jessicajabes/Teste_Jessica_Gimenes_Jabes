"""
Modelos Pydantic para validação e serialização de dados
"""
from pydantic import BaseModel, Field
from typing import Optional, List, Any
from datetime import datetime


# === OPERADORAS ===

class OperadoraBase(BaseModel):
    """Schema base de operadora"""
    reg_ans: str
    cnpj: str
    razao_social: str
    modalidade: Optional[str] = None
    uf: Optional[str] = None
    status: str


class OperadoraListItem(OperadoraBase):
    """Item da lista de operadoras (resumido)"""
    pass


class OperadoraDetalhes(OperadoraBase):
    """Detalhes completos de uma operadora"""
    data_carga: Optional[datetime] = None


# === DESPESAS ===

class DespesaItem(BaseModel):
    """Item de despesa"""
    reg_ans: str
    razao_social: str
    uf: Optional[str] = None
    trimestre: str
    total_despesas: float
    media_despesas_trimestre: float
    qtd_trimestres: int


# === ESTATÍSTICAS ===

class TopOperadora(BaseModel):
    """Top operadora por despesas"""
    razao_social: str
    uf: Optional[str] = None
    total_despesas: float


class DespesaPorUF(BaseModel):
    """Despesas agregadas por UF"""
    uf: str
    total_despesas: float
    qtd_operadoras: int


class Estatisticas(BaseModel):
    """Estatísticas gerais"""
    total_operadoras: int
    total_operadoras_ativas: int
    total_operadoras_canceladas: int
    total_despesas_geral: float
    media_despesas: float
    top_5_operadoras: List[TopOperadora]
    despesas_por_uf: List[DespesaPorUF]


# === PAGINAÇÃO ===

class PaginationMetadata(BaseModel):
    """Metadados de paginação"""
    total: int
    page: int
    limit: int
    total_pages: int


class PaginatedResponse(BaseModel):
    """Resposta paginada genérica"""
    data: List[Any]
    metadata: PaginationMetadata


# === RESPOSTAS DE ERRO ===

class ErrorResponse(BaseModel):
    """Resposta de erro padrão"""
    error: str
    detail: Optional[str] = None
    status_code: int
