#!/usr/bin/env python
import psycopg2

try:
    conn = psycopg2.connect(
        host='localhost',
        port=55432,
        database='intuitive_care',
        user='jessica',
        password='1234'
    )
    
    cursor = conn.cursor()
    cursor.execute('DELETE FROM demonstracoes_contabeis_temp')
    conn.commit()
    
    cursor.execute('SELECT COUNT(*) FROM demonstracoes_contabeis_temp')
    count = cursor.fetchone()[0]
    print(f'Tabela limpa! Registros atuais: {count}')
    
    cursor.close()
    conn.close()
    
except Exception as e:
    print(f'Erro: {e}')
