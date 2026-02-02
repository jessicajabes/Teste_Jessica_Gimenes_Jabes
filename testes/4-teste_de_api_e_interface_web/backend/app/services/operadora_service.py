"""
Serviço de Operadoras
Contém toda a lógica de negócio para operadoras
"""
from typing import List, Optional, Dict, Any
import math
from app.database.connection import get_db_connection
from app.models.schemas import (
    OperadoraListItem, 
    OperadoraDetalhes, 
    PaginatedResponse, 
    PaginationMetadata
)


class OperadoraService:
    """Serviço para operações com operadoras"""
    
    @staticmethod
    def listar_operadoras(
        page: int = 1, 
        limit: int = 10, 
        search: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Lista operadoras com paginação e busca
        
        TRADE-OFF: Estratégia de Paginação
        DECISÃO: Offset-based (Opção A)
        
        JUSTIFICATIVA:
        - Dados são relativamente estáveis (operadoras não mudam frequentemente)
        - Implementação simples e bem compreendida
        - Suporte nativo no PostgreSQL (LIMIT/OFFSET)
        - Adequado para <100k registros
        - Permite pular para páginas específicas facilmente
        
        QUANDO USAR ALTERNATIVAS:
        - Cursor-based: Datasets muito grandes (>1M registros) com ordenação por ID
        - Keyset: Necessidade de ordenação complexa e datasets grandes
        """
        offset = (page - 1) * limit
        
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                # Query base
                where_clause = ""
                params = []
                
                if search:
                    where_clause = """
                    WHERE LOWER(razao_social) LIKE LOWER(%s) 
                       OR cnpj LIKE %s
                    """
                    search_param = f"%{search}%"
                    params = [search_param, search_param]
                
                # Contar total
                count_query = f"SELECT COUNT(*) as total FROM operadoras {where_clause}"
                cur.execute(count_query, params)
                total = cur.fetchone()['total']
                
                # Buscar dados paginados
                query = f"""
                    SELECT 
                        reg_ans,
                        cnpj,
                        razao_social,
                        modalidade,
                        uf,
                        status
                    FROM operadoras
                    {where_clause}
                    ORDER BY razao_social
                    LIMIT %s OFFSET %s
                """
                cur.execute(query, params + [limit, offset])
                operadoras = cur.fetchall()
                
                # Calcular total de páginas
                total_pages = math.ceil(total / limit) if total > 0 else 0
                
                return {
                    "data": [dict(op) for op in operadoras],
                    "metadata": {
                        "total": total,
                        "page": page,
                        "limit": limit,
                        "total_pages": total_pages
                    }
                }
    
    @staticmethod
    def obter_operadora_por_cnpj(cnpj: str) -> Optional[Dict[str, Any]]:
        """
        Obtém detalhes de uma operadora por CNPJ
        """
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                query = """
                    SELECT 
                        reg_ans,
                        cnpj,
                        razao_social,
                        modalidade,
                        uf,
                        status,
                        data_carga
                    FROM operadoras
                    WHERE cnpj = %s
                """
                cur.execute(query, (cnpj,))
                operadora = cur.fetchone()
                
                return dict(operadora) if operadora else None
    
    @staticmethod
    def obter_despesas_operadora(cnpj: str) -> List[Dict[str, Any]]:
        """
        Obtém histórico de despesas de uma operadora
        """
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                # Buscar reg_ans pelo CNPJ
                cur.execute("SELECT reg_ans FROM operadoras WHERE cnpj = %s", (cnpj,))
                resultado = cur.fetchone()
                
                if not resultado:
                    return []
                
                reg_ans = resultado['reg_ans']
                
                # Buscar despesas (sem dedução e com dedução)
                query = """
                    SELECT 
                        reg_ans,
                        razao_social,
                        uf,
                        trimestre,
                        total_despesas,
                        media_despesas_trimestre,
                        qtd_trimestres,
                        'sem_deducao' as tipo
                    FROM despesas_agregadas
                    WHERE reg_ans = %s
                    
                    UNION ALL
                    
                    SELECT 
                        reg_ans,
                        razao_social,
                        uf,
                        trimestre,
                        total_despesas,
                        media_despesas_trimestre,
                        qtd_trimestres,
                        'com_deducao' as tipo
                    FROM despesas_agregadas_c_deducoes
                    WHERE reg_ans = %s
                    
                    ORDER BY trimestre, tipo
                """
                cur.execute(query, (reg_ans, reg_ans))
                despesas = cur.fetchall()
                
                return [dict(d) for d in despesas]
