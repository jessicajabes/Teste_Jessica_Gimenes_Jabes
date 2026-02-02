"""
Serviço de Estatísticas
Contém lógica para cálculo de estatísticas agregadas
"""
from typing import Dict, Any
from app.database.connection import get_db_connection
from app.services.cache_service import get_cached, set_cache


class EstatisticasService:
    """Serviço para estatísticas agregadas"""
    
    CACHE_KEY = "estatisticas_gerais"
    
    @staticmethod
    def obter_estatisticas() -> Dict[str, Any]:
        """
        Obtém estatísticas gerais do sistema
        
        TRADE-OFF: Cache vs Queries Diretas
        DECISÃO: Cachear resultado por X minutos (Opção B)
        
        JUSTIFICATIVA:
        - Cálculos de agregação são custosos (múltiplas JOINs e SUM)
        - Dados não mudam com frequência (batch diário/semanal)
        - TTL de 5 minutos oferece balance entre performance e atualização
        - Reduz carga no banco de dados significativamente
        - Mais flexível que pré-calcular em tabela (Opção C)
        
        IMPLEMENTAÇÃO:
        - Primeiro verifica cache
        - Se não houver cache, calcula e armazena
        - Cache expira automaticamente após TTL
        
        QUANDO USAR PRÉ-CÁLCULO (Opção C):
        - Se estatísticas são consultadas em dashboards críticos
        - Se dados mudam em horários específicos (pode-se recalcular via cron)
        - Se múltiplas instâncias da API precisam compartilhar cache
        """
        # Verificar cache primeiro
        cached_result = get_cached(EstatisticasService.CACHE_KEY)
        if cached_result:
            return cached_result
        
        # Calcular estatísticas
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                # Total de operadoras
                cur.execute("SELECT COUNT(*) as total FROM operadoras")
                total_operadoras = cur.fetchone()['total']
                
                # Operadoras ativas
                cur.execute("SELECT COUNT(*) as total FROM operadoras WHERE status = 'ATIVA'")
                total_ativas = cur.fetchone()['total']
                
                # Operadoras canceladas
                cur.execute("SELECT COUNT(*) as total FROM operadoras WHERE status = 'CANCELADA'")
                total_canceladas = cur.fetchone()['total']
                
                # Total de despesas e média
                cur.execute("""
                    SELECT 
                        SUM(total_despesas) as total_despesas,
                        AVG(media_despesas_trimestre) as media_despesas
                    FROM despesas_agregadas
                """)
                despesas_stats = cur.fetchone()
                
                # Top 5 operadoras por despesas
                cur.execute("""
                    SELECT 
                        razao_social,
                        uf,
                        total_despesas
                    FROM despesas_agregadas
                    ORDER BY total_despesas DESC
                    LIMIT 5
                """)
                top_operadoras = [dict(row) for row in cur.fetchall()]
                
                # Despesas por UF
                cur.execute("""
                    SELECT 
                        uf,
                        SUM(total_despesas) as total_despesas,
                        COUNT(DISTINCT razao_social) as qtd_operadoras
                    FROM despesas_agregadas
                    WHERE uf IS NOT NULL
                    GROUP BY uf
                    ORDER BY total_despesas DESC
                """)
                despesas_por_uf = [dict(row) for row in cur.fetchall()]
                
                resultado = {
                    "total_operadoras": total_operadoras,
                    "total_operadoras_ativas": total_ativas,
                    "total_operadoras_canceladas": total_canceladas,
                    "total_despesas_geral": float(despesas_stats['total_despesas'] or 0),
                    "media_despesas": float(despesas_stats['media_despesas'] or 0),
                    "top_5_operadoras": top_operadoras,
                    "despesas_por_uf": despesas_por_uf
                }
                
                # Armazenar no cache
                set_cache(EstatisticasService.CACHE_KEY, resultado)
                
                return resultado
