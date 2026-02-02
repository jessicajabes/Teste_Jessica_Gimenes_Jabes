"""Repositório de acesso ao banco"""
from typing import Optional, List, Dict, Any
from sqlalchemy import text
from database import engine


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
        ORDER BY razao_social
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

    return {"total": total, "data": [dict(row) for row in rows]}


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
    sql = text(
        """
        WITH operadora AS (
            SELECT razao_social
            FROM operadoras
            WHERE cnpj = :cnpj
        )
        SELECT
            'SEM DEDUÇÃO' AS tipo_deducao,
            d.razao_social,
            d.uf,
            d.total_despesas,
            d.media_despesas_trimestre,
            d.desvio_padrao_despesas,
            d.qtd_registros,
            d.qtd_trimestres,
            d.qtd_anos
        FROM despesas_agregadas d
        WHERE d.razao_social = (SELECT razao_social FROM operadora)

        UNION ALL

        SELECT
            'COM DEDUÇÃO' AS tipo_deducao,
            d.razao_social,
            d.uf,
            d.total_despesas,
            d.media_despesas_trimestre,
            d.desvio_padrao_despesas,
            d.qtd_registros,
            d.qtd_trimestres,
            d.qtd_anos
        FROM despesas_agregadas_c_deducoes d
        WHERE d.razao_social = (SELECT razao_social FROM operadora)
        """
    )

    with engine.connect() as conn:
        rows = conn.execute(sql, {"cnpj": cnpj}).mappings().all()
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

    with engine.connect() as conn:
        totais = conn.execute(sql_totais).mappings().first()
        top5 = conn.execute(sql_top5).mappings().all()
        por_uf = conn.execute(sql_por_uf).mappings().all()

    return {
        "total_despesas": float(totais["total_despesas"] or 0),
        "media_despesas": float(totais["media_despesas"] or 0),
        "top_5_operadoras": [dict(row) for row in top5],
        "despesas_por_uf": [dict(row) for row in por_uf],
    }
