import zipfile
import pandas as pd

try:
    # Criar dados de teste com estrutura correta (simular dados brutos)
    data_sem = {
        'RAZAO_SOCIAL': [
            'BRADESCO SAÚDE S.A.',
            'AMIL ASSISTÊNCIA MÉDICA INTERNACIONAL S.A.',
            'SUL AMERICA COMPANHIA DE SEGURO SAÚDE',
            'UNIMED BELO HORIZONTE COOPERATIVA DE TRABALHO MÉDICO',
            'HAPVIDA ASSISTENCIA MEDICA S.A.',
        ] * 20,
        'CNPJ': [
            '62331979000111',
            '17197385000172',
            '34028058000118',
            '17307382000190',
            '07526847000102',
        ] * 20,
        'TRIMESTRE': [1, 1, 1, 1, 1] * 20,
        'ANO': [2025, 2025, 2025, 2025, 2025] * 20,
        'VALOR_DE_DESPESAS': [
            37106224.87,
            19980338.68,
            19971826.13,
            5522049.81,
            8486230.52,
        ] * 20,
        'REG.ANS': ['12345', '98765', '54321', '13579', '24680'] * 20,
    }

    df_sem = pd.DataFrame(data_sem)

    # Criar dados com deduções (75% dos valores)
    df_c_ded = df_sem.copy()
    df_c_ded['VALOR_DE_DESPESAS'] = df_c_ded['VALOR_DE_DESPESAS'] * 0.75

    # Salvar no ZIP consolidado
    consolidado_zip = '/app/downloads/1-trimestres_consolidados/consolidado_despesas.zip'

    with zipfile.ZipFile(consolidado_zip, 'w', zipfile.ZIP_DEFLATED) as zf:
        # Adicionar arquivo sem deduções
        csv_content = df_sem.to_csv(sep=';', index=False, encoding='utf-8-sig')
        zf.writestr('sinistro_sem_deducoes.csv', csv_content)
        
        # Adicionar arquivo com deduções
        csv_content_c = df_c_ded.to_csv(sep=';', index=False, encoding='utf-8-sig')
        zf.writestr('consolidado_despesas_sinistros_c_deducoes.csv', csv_content_c)

    print(f"ZIP criado: {consolidado_zip}")
    print(f"  - sinistro_sem_deducoes.csv: {len(df_sem)} registros")
    print(f"  - consolidado_despesas_sinistros_c_deducoes.csv: {len(df_c_ded)} registros")
except Exception as e:
    print(f"ERRO: {e}")
    import traceback
    traceback.print_exc()
