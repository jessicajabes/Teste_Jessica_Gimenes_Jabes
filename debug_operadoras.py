from sqlalchemy import create_engine
import pandas as pd

db_url = "postgresql+psycopg2://postgres:postgres@intuitive-care:5432/intuitive_care"
engine = create_engine(db_url)

df = pd.read_sql_query("SELECT reg_ans, uf, modalidade FROM operadoras LIMIT 15", engine)
print("Dados do banco:")
print(df)
print("\nUnique UFs:")
print(df['uf'].unique())
print("\nNull UF count:")
print(df['uf'].isnull().sum())
engine.dispose()
