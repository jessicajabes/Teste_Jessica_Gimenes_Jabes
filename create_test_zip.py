import zipfile
import pandas as pd
import os

# Criar dados de exemplo para teste
data = {
    'RAZAO_SOCIAL': ['BRADESCO SAÚDE S.A.', 'AMIL SAÚDE S.A.'],
    'CNPJ': ['12345678000100', '98765432000200'],
    'TRIMESTRE': [1, 1],
    'ANO': [2025, 2025],
    'VALOR_DE_DESPESAS': [37106224480.70, 19980338683.88],
    'REG. ANS': ['12345', '98765'],
}

df = pd.DataFrame(data)

# Salvar em um ZIP consolidado
consolidado_zip = '/app/downloads/1-trimestres_consolidados/consolidado_despesas.zip'

with zipfile.ZipFile(consolidado_zip, 'w', zipfile.ZIP_DEFLATED) as zf:
    # Adicionar arquivo sem deduções
    csv_content = df.to_csv(sep=';', index=False, encoding='utf-8-sig')
    zf.writestr('sinistro_sem_deducoes.csv', csv_content)
    
    # Adicionar arquivo com deduções (com dados ligeiramente diferentes)
    csv_content_c = df.copy()
    csv_content_c['VALOR_DE_DESPESAS'] = csv_content_c['VALOR_DE_DESPESAS'] * 0.8
    zf.writestr('consolidado_despesas_sinistros_c_deducoes.csv', csv_content_c.to_csv(sep=';', index=False, encoding='utf-8-sig'))

print(f"ZIP criado: {consolidado_zip}")
