import zipfile
import io

# Abrir ZIP
zip_path = "/app/downloads/2-tranformacao_validacao/Teste_Jessica_Jabes.zip"

with zipfile.ZipFile(zip_path, 'r') as zipf:
    # Ler primeiro CSV
    with zipf.open('despesas_agregadas.csv') as f:
        # Testar diferentes encodings
        content_bytes = f.read()
        
        print("=== Testando encodings ===")
        print(f"Tamanho: {len(content_bytes)} bytes")
        print()
        
        # UTF-8
        try:
            content_utf8 = content_bytes.decode('utf-8')
            lines = content_utf8.split('\n')[:10]
            print("UTF-8:")
            for line in lines:
                print(f"  {line}")
            print()
        except:
            print("UTF-8: ERRO\n")
        
        # Latin-1
        try:
            content_latin1 = content_bytes.decode('latin-1')
            lines = content_latin1.split('\n')[:10]
            print("Latin-1:")
            for line in lines:
                print(f"  {line}")
            print()
        except:
            print("Latin-1: ERRO\n")
        
        # CP1252 (Windows)
        try:
            content_cp1252 = content_bytes.decode('cp1252')
            lines = content_cp1252.split('\n')[:10]
            print("CP1252:")
            for line in lines:
                print(f"  {line}")
        except:
            print("CP1252: ERRO")
