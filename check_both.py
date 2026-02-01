import zipfile
import pandas as pd
import io

# Verificar arquivo de entrada
zip_input = "/app/downloads/1-trimestres_consolidados/consolidado_despesas.zip"

print("=== ARQUIVO DE ENTRADA ===")
with zipfile.ZipFile(zip_input, 'r') as zipf:
    with zipf.open('sinistro_sem_deducoes.csv') as f:
        content_bytes = f.read()
        
        # Tentar UTF-8
        try:
            df = pd.read_csv(io.BytesIO(content_bytes), sep=';', encoding='utf-8', nrows=5)
            print("UTF-8:")
            print(df[['RAZAO_SOCIAL']].to_string())
            print()
        except:
            print("UTF-8: ERRO\n")
        
        # Tentar Latin-1
        try:
            df = pd.read_csv(io.BytesIO(content_bytes), sep=';', encoding='latin-1', nrows=5)
            print("Latin-1:")
            print(df[['RAZAO_SOCIAL']].to_string())
            print()
        except:
            print("Latin-1: ERRO\n")
        
        # Tentar CP1252
        try:
            df = pd.read_csv(io.BytesIO(content_bytes), sep=';', encoding='cp1252', nrows=5)
            print("CP1252:")
            print(df[['RAZAO_SOCIAL']].to_string())
        except:
            print("CP1252: ERRO")

print("\n=== ARQUIVO DE SAÍDA ===")
zip_output = "/app/downloads/2-tranformacao_validacao/Teste_Jessica_Jabes.zip"

with zipfile.ZipFile(zip_output, 'r') as zipf:
    with zipf.open('despesas_agregadas.csv') as f:
        content_bytes = f.read()
        
        # Tentar UTF-8
        try:
            df = pd.read_csv(io.BytesIO(content_bytes), sep=';', encoding='utf-8', nrows=5)
            print("UTF-8:")
            print(df[['razao_social']].to_string())
        except Exception as e:
            print(f"UTF-8: ERRO - {e}")
