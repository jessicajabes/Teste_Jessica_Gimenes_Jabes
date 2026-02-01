import zipfile

zip_path = '/app/downloads/Teste_Jessica_Jabes/despesas_agregadas.zip'
with zipfile.ZipFile(zip_path, 'r') as z:
    print("Arquivos no ZIP:")
    for info in z.filelist:
        print(f"  {info.filename} ({info.file_size} bytes)")
    print("\nPrimeiras linhas do CSV sem deduções:")
    with z.open('despesas_agregadas.csv') as f:
        for i, line in enumerate(f):
            if i < 5:
                print(f"  {line.decode('utf-8').rstrip()}")
            else:
                break
