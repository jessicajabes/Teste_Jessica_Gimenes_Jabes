import zipfile
import pandas as pd
import io

try:
    # Extrair dados reais do primeiro trimestre
    trimestre_zip = '/app/downloads/arquivos_trimestres/1T2025.zip'
    consolidado_zip = '/app/downloads/1-trimestres_consolidados/consolidado_despesas.zip'

    print("Iniciando...")

    # Ler arquivo do trimestre
    with zipfile.ZipFile(trimestre_zip, 'r') as zin:
        # Listar arquivos
        files = zin.namelist()
        print(f"Arquivos no trimestre: {files}")
        
        # Ler o CSV
        for fname in files:
            if fname.endswith('.csv'):
                print(f"Lendo: {fname}")
                with zin.open(fname) as f:
                    df = pd.read_csv(f, sep=';', encoding='utf-8-sig', nrows=1000)  # Ler apenas 1000 linhas
                    print(f"Linhas lidas: {len(df)}")
                    print(f"Primeiras colunas: {df.columns[:10].tolist()}")
                    
                    # Dividir em dois DataFrames
                    df1 = df.head(int(len(df) * 0.5))
                    df2 = df.tail(int(len(df) * 0.5))
                    
                    # Salvar no ZIP consolidado
                    with zipfile.ZipFile(consolidado_zip, 'w', zipfile.ZIP_DEFLATED) as zout:
                        # Adicionar como "sinistro_sem_deducoes"
                        csv1 = df1.to_csv(sep=';', index=False, encoding='utf-8-sig')
                        zout.writestr('sinistro_sem_deducoes.csv', csv1)
                        
                        # Adicionar como "consolidado_despesas_sinistros_c_deducoes"
                        csv2 = df2.to_csv(sep=';', index=False, encoding='utf-8-sig')
                        zout.writestr('consolidado_despesas_sinistros_c_deducoes.csv', csv2)
                    
                    print(f"ZIP consolidado criado com {len(df1)} e {len(df2)} registros")
                    break
    print("Concluído!")
except Exception as e:
    print(f"ERRO: {e}")
    import traceback
    traceback.print_exc()
