import sqlalchemy
from sqlalchemy import create_engine, text

engine = create_engine("postgresql://jessica:1234@postgres:5432/intuitive_care")

with engine.connect() as conn:
    # Limpar operadoras antigas
    conn.execute(text("DELETE FROM operadoras"))
    
    # Inserir operadoras de teste
    operadoras = [
        ('62331979000111', '12345', 'Operadora', 'RJ', 'ATIVA'),
        ('17197385000172', '98765', 'Cooperativa', 'SP', 'ATIVA'),
        ('34028058000118', '54321', 'Operadora', 'RJ', 'ATIVA'),
        ('17307382000190', '13579', 'Cooperativa', 'MG', 'ATIVA'),
        ('07526847000102', '24680', 'Operadora', 'CE', 'ATIVA'),
        ('88049177000140', '34567', 'Cooperativa', 'SP', 'ATIVA'),
        ('60873980000107', '45678', 'Operadora', 'SP', 'ATIVA'),
        ('36113102000160', '56789', 'Operadora', 'DF', 'ATIVA'),
        ('61696362000119', '67890', 'Operadora', 'SP', 'ATIVA'),
        ('33105264000119', '90123', 'Operadora', 'DF', 'ATIVA'),
        ('34895646000147', '23457', 'Operadora', 'RJ', 'ATIVA'),
        ('96265589000180', '34568', 'Cooperativa', 'RS', 'ATIVA'),
        ('76535764000148', '45679', 'Operadora', 'PR', 'ATIVA'),
        ('47728218000106', '56790', 'Cooperativa', 'SP', 'ATIVA'),
        ('82825370000108', '78902', 'Cooperativa', 'PR', 'ATIVA'),
        ('08723521000164', '89013', 'Cooperativa', 'CE', 'ATIVA'),
    ]
    
    for cnpj, reg_ans, modalidade, uf, status in operadoras:
        try:
            conn.execute(text("""
                INSERT INTO operadoras (cnpj, reg_ans, modalidade, uf, status)
                VALUES (:cnpj, :reg_ans, :modalidade, :uf, :status)
            """), {"cnpj": cnpj, "reg_ans": reg_ans, "modalidade": modalidade, "uf": uf, "status": status})
        except:
            pass
    
    conn.commit()
    
    # Verificar
    result = conn.execute(text("SELECT COUNT(*) FROM operadoras"))
    count = result.scalar()
    print(f"Operadoras inseridas: {count}")
    
    # Listar algumas
    result = conn.execute(text("SELECT reg_ans, uf FROM operadoras LIMIT 10"))
    print("Primeiras operadoras:")
    for row in result:
        print(f"  {row[0]}: {row[1]}")
