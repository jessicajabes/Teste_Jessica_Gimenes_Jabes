
from fastapi import APIRouter, HTTPException
from app.services.estatisticas_service import EstatisticasService

router = APIRouter(prefix="/api", tags=["Estatísticas"])
service = EstatisticasService()


@router.get(
    "/estatisticas",
    summary="Estatísticas gerais",
    description="""
    Retorna estatísticas agregadas do sistema.
    
    **Inclui**:
    - Total de operadoras (ativas e canceladas)
    - Total e média de despesas
    - Top 5 operadoras por despesas
    - Distribuição de despesas por UF
    
    **Performance**: Resultado cacheado por 5 minutos
    """
)
async def obter_estatisticas():

    try:
        estatisticas = service.obter_estatisticas()
        return estatisticas
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro ao calcular estatísticas: {str(e)}")
