
from fastapi import APIRouter, HTTPException, Query
from typing import Optional
from app.services.operadora_service import OperadoraService
from app.models.schemas import ErrorResponse

router = APIRouter(prefix="/api/operadoras", tags=["Operadoras"])
service = OperadoraService()


@router.get(
    "",
    summary="Listar operadoras",
    description="""
    Lista todas as operadoras com paginação e busca opcional.
    
    **Paginação**: Offset-based (LIMIT/OFFSET)
    
    **Busca**: Filtra por razão social ou CNPJ (case-insensitive, parcial)
    
    **Resposta**: Inclui dados + metadados de paginação
    """
)
async def listar_operadoras(
    page: int = Query(1, ge=1, description="Número da página (inicia em 1)"),
    limit: int = Query(10, ge=1, le=100, description="Itens por página (máx: 100)"),
    search: Optional[str] = Query(None, description="Buscar por razão social ou CNPJ")
):

    try:
        resultado = service.listar_operadoras(page, limit, search)
        return resultado
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro ao listar operadoras: {str(e)}")


@router.get(
    "/{cnpj}",
    summary="Detalhes da operadora",
    description="""
    Retorna informações detalhadas de uma operadora específica.
    
    **Busca**: Por CNPJ (sem formatação, apenas números)
    """
)
async def obter_operadora(cnpj: str):

    try:
        operadora = service.obter_operadora_por_cnpj(cnpj)
        
        if not operadora:
            raise HTTPException(
                status_code=404,
                detail=f"Operadora com CNPJ {cnpj} não encontrada"
            )
        
        return operadora
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro ao buscar operadora: {str(e)}")


@router.get(
    "/{cnpj}/despesas",
    summary="Histórico de despesas",
    description="""
    Retorna o histórico completo de despesas de uma operadora.
    
    **Inclui**:
    - Despesas sem dedução
    - Despesas com dedução
    
    **Agrupado por**: Trimestre
    """
)
async def obter_despesas_operadora(cnpj: str):

    try:
        despesas = service.obter_despesas_operadora(cnpj)
        
        if not despesas:
            # Verificar se operadora existe
            operadora = service.obter_operadora_por_cnpj(cnpj)
            if not operadora:
                raise HTTPException(
                    status_code=404,
                    detail=f"Operadora com CNPJ {cnpj} não encontrada"
                )
            # Operadora existe mas não tem despesas
            return []
        
        return despesas
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro ao buscar despesas: {str(e)}")
