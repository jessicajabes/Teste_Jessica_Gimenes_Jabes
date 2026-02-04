"""Modelos de resposta da API"""
from typing import List, Optional
from pydantic import BaseModel


class Operadora(BaseModel):
    reg_ans: str
    cnpj: str
    razao_social: str
    modalidade: Optional[str] = None
    uf: Optional[str] = None
    status: str


class OperadoraDetalhe(Operadora):
    pass


class DespesaOperadora(BaseModel):
    tipo_deducao: str
    razao_social: str
    uf: Optional[str]
    total_despesas: float
    media_despesas_trimestre: float
    desvio_padrao_despesas: float
    qtd_registros: int
    qtd_trimestres: int
    qtd_anos: int


class PaginacaoOperadoras(BaseModel):
    data: List[Operadora]
    total: int
    page: int
    limit: int


class TopOperadora(BaseModel):
    razao_social: str
    uf: Optional[str]
    total_despesas: float


class DespesasPorUf(BaseModel):
    uf: str
    total_despesas: float


class Estatisticas(BaseModel):
    total_despesas: float
    media_despesas: float
    top_5_operadoras: List[TopOperadora]
    despesas_por_uf: List[DespesasPorUf]
