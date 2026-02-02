"""
Router de Estatísticas
Define rotas para estatísticas agregadas
"""
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
    """
    Obtém estatísticas gerais do sistema
    
    Returns:
        Objeto com estatísticas agregadas
        
    Note:
        Esta rota utiliza cache com TTL de 5 minutos para otimizar performance
    """
    try:
        estatisticas = service.obter_estatisticas()
        return estatisticas
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro ao calcular estatísticas: {str(e)}")
