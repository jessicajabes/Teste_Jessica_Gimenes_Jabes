import zipfile

zip_path = '/app/downloads/Teste_Jessica_Jabes/despesas_agregadas.zip'
with zipfile.ZipFile(zip_path, 'r') as z:
    data = z.read('despesas_agregadas.csv').decode('utf-8')
    lines = data.split('\n')
    for line in lines[:10]:
        print(line)
