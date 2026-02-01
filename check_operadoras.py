import sqlalchemy
from sqlalchemy import create_engine, text

engine = create_engine("postgresql://jessica:1234@postgres:5432/intuitive_care")

with engine.connect() as conn:
    result = conn.execute(text("""
        SELECT reg_ans, status, COUNT(*) as qtd 
        FROM operadoras 
        GROUP BY reg_ans, status 
        ORDER BY qtd DESC 
        LIMIT 30
    """))
    for row in result:
        print(f"reg_ans={row[0]}, status={row[1]}, count={row[2]}")
