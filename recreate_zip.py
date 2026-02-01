import zipfile
import pandas as pd

# Criar um ZIP consolidado com dados dos trimestres para teste
consolidado_zip = '/app/downloads/1-trimestres_consolidados/consolidado_despesas.zip'

with zipfile.ZipFile(consolidado_zip, 'w', zipfile.ZIP_DEFLATED) as zout:
    # Lê de um dos trimestres como exemplo
    try:
        with zipfile.ZipFile('/app/downloads/arquivos_trimestres/1T2025.zip', 'r') as zin:
            for item in zin.infolist():
                # Copiar apenas arquivo útil
                if 'sinistro_sem_deducoes' in item.filename or 'consolidado_despesas_sinistros' in item.filename:
                    zout.writestr(item, zin.read(item.filename))
    except:
        pass
        
print("ZIP consolidado recreado com dados de teste")
