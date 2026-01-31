#!/usr/bin/env python
import psycopg2
import os

try:
    # Usar localhost para conectar do host Windows via port forwarding
    conn = psycopg2.connect(
        host='localhost',
        port=5432,
        database='ans_db',
        user='ans_user',
        password='ans_password'
    )
    
    cursor = conn.cursor()
    
    # Query para verificar dados
    cursor.execute("""
        SELECT 
            COUNT(*) as total_registros,
            COUNT(valor_trimestre) as registros_com_valor,
            MIN(valor_trimestre) as min_valor,
            MAX(valor_trimestre) as max_valor,
            SUM(valor_trimestre) as soma_valor
        FROM demonstracoes_contabeis_temp
    """)
    
    resultado = cursor.fetchone()
    
    print("=" * 60)
    print("DADOS NA TABELA demonstracoes_contabeis_temp")
    print("=" * 60)
    print(f"Total registros: {resultado[0]}")
    print(f"Registros com valor_trimestre: {resultado[1]}")
    print(f"MIN(valor_trimestre): {resultado[2]}")
    print(f"MAX(valor_trimestre): {resultado[3]}")
    print(f"SUM(valor_trimestre): {resultado[4]}")
    print("=" * 60)
    
    cursor.close()
    conn.close()
    
except Exception as e:
    print(f"ERRO: {e}")
    import traceback
    traceback.print_exc()
