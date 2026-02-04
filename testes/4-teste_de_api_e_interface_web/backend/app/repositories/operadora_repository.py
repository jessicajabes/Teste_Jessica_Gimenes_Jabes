"""Repositório de acesso ao banco"""
from typing import Optional, List, Dict, Any
import math
from sqlalchemy import text
from app.core.database import engine


def listar_operadoras(page: int, limit: int, busca: Optional[str]) -> Dict[str, Any]:

    offset = (page - 1) * limit

    filtros = ""
    params = {"limit": limit, "offset": offset}
    if busca:
        filtros = "WHERE razao_social ILIKE :busca OR cnpj ILIKE :busca"
        params["busca"] = f"%{busca}%"

    sql_data = text(
        f"""
        SELECT reg_ans, cnpj, razao_social, modalidade, uf, status
        FROM operadoras
        {filtros}
        ORDER BY status, razao_social
        LIMIT :limit OFFSET :offset
        """
    )

    sql_total = text(
        f"""
        SELECT COUNT(*) AS total
        FROM operadoras
        {filtros}
        """
    )

    with engine.connect() as conn:
        total = conn.execute(sql_total, params).mappings().first()["total"]
        rows = conn.execute(sql_data, params).mappings().all()
    
    total_pages = math.ceil(total / limit) if total > 0 else 0

    return {
        "total": total,
        "data": [dict(row) for row in rows],
        "page": page,
        "limit": limit,
        "total_pages": total_pages
    }


def obter_operadora_por_cnpj(cnpj: str) -> Optional[Dict[str, Any]]:

    sql = text(
        """
        SELECT reg_ans, cnpj, razao_social, modalidade, uf, status
        FROM operadoras
        WHERE cnpj = :cnpj
        """
    )
    with engine.connect() as conn:
        row = conn.execute(sql, {"cnpj": cnpj}).mappings().first()
    return dict(row) if row else None


def obter_despesas_por_cnpj(cnpj: str) -> List[Dict[str, Any]]:

    sql_reg_ans = text(
        """
        SELECT reg_ans
        FROM operadoras
        WHERE cnpj = :cnpj
        """
    )
    
    with engine.connect() as conn:
        resultado = conn.execute(sql_reg_ans, {"cnpj": cnpj}).mappings().first()
        
        if not resultado:
            return []
        
        reg_ans = resultado['reg_ans']
        
        # Buscar despesas detalhadas por trimestre/ano
        sql_despesas = text(
            """
            SELECT
                'SEM DEDUÇÃO' AS tipo_deducao,
                reg_ans,
                razao_social,
                cnpj,
                trimestre,
                ano,
                valor_despesas AS valor
            FROM consolidados_despesas
            WHERE reg_ans = :reg_ans

            UNION ALL

            SELECT
                'COM DEDUÇÃO' AS tipo_deducao,
                reg_ans,
                razao_social,
                cnpj,
                trimestre,
                ano,
                valor_despesas AS valor
            FROM consolidados_despesas_c_deducoes
            WHERE reg_ans = :reg_ans
            
            ORDER BY tipo_deducao DESC, ano, trimestre
            """
        )
        
        rows = conn.execute(sql_despesas, {"reg_ans": reg_ans}).mappings().all()
    
    return [dict(row) for row in rows]


def obter_estatisticas() -> Dict[str, Any]:
    sql_totais = text(
        """
        SELECT
            SUM(total_despesas) AS total_despesas,
            AVG(total_despesas) AS media_despesas
        FROM despesas_agregadas
        """
    )

    sql_top5 = text(
        """
        SELECT razao_social, uf, total_despesas
        FROM despesas_agregadas
        ORDER BY total_despesas DESC
        LIMIT 5
        """
    )

    sql_por_uf = text(
        """
        SELECT uf, SUM(total_despesas) AS total_despesas
        FROM despesas_agregadas
        WHERE uf IS NOT NULL
        GROUP BY uf
        ORDER BY total_despesas DESC
        """
    )

    sql_totais_c_deducoes = text(
        """
        SELECT
            SUM(total_despesas) AS total_despesas,
            AVG(total_despesas) AS media_despesas
        FROM despesas_agregadas_c_deducoes
        """
    )

    sql_top5_c_deducoes = text(
        """
        SELECT razao_social, uf, total_despesas
        FROM despesas_agregadas_c_deducoes
        ORDER BY total_despesas DESC
        LIMIT 5
        """
    )

    sql_por_uf_c_deducoes = text(
        """
        SELECT uf, SUM(total_despesas) AS total_despesas
        FROM despesas_agregadas_c_deducoes
        WHERE uf IS NOT NULL
        GROUP BY uf
        ORDER BY total_despesas DESC
        """
    )

    with engine.connect() as conn:
        totais = conn.execute(sql_totais).mappings().first()
        top5 = conn.execute(sql_top5).mappings().all()
        por_uf = conn.execute(sql_por_uf).mappings().all()
        totais_c_deducoes = conn.execute(sql_totais_c_deducoes ).mappings().first()
        top5_c_deducoes  = conn.execute(sql_top5_c_deducoes ).mappings().all()
        por_uf_c_deducoes  = conn.execute(sql_por_uf_c_deducoes ).mappings().all()        

    return {
        "total_despesas": float(totais["total_despesas"] or 0),
        "media_despesas": float(totais["media_despesas"] or 0),
        "top_5_operadoras": [dict(row) for row in top5],
        "despesas_por_uf": [dict(row) for row in por_uf],
        "total_despesas_c_deducoes ": float(totais_c_deducoes ["total_despesas"] or 0),
        "media_despesas_c_deducoes ": float(totais_c_deducoes ["media_despesas"] or 0),
        "top_5_operadoras_c_deducoes ": [dict(row) for row in top5_c_deducoes ],
        "despesas_por_uf_c_deducoes ": [dict(row) for row in por_uf_c_deducoes ],
    }
