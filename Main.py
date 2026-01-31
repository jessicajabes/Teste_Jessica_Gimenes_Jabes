"""
Main.py - Ponto de entrada do sistema

Este arquivo orquestra a execução das diferentes funcionalidades do sistema.
"""

import sys
import os
import importlib.util

def carregar_modulo(caminho_arquivo, nome_modulo):
    """Carrega um módulo Python dinamicamente"""
    spec = importlib.util.spec_from_file_location(nome_modulo, caminho_arquivo)
    modulo = importlib.util.module_from_spec(spec)
    sys.modules[nome_modulo] = modulo
    spec.loader.exec_module(modulo)
    return modulo

def main():
    print("="*70)
    print("SISTEMA DE ANÁLISE DE DADOS - ANS")
    print("="*70)
    print("\nFuncionalidades disponíveis:")
    print("1. Teste de Integração com API Pública")
    print("2. Transformação e Validação de Dados")
    print("="*70)
    
    # Obter o diretório base
    base_dir = os.path.dirname(__file__)
    
    # Executar a integração com API pública
    print("\n[INICIANDO] 1. Teste de Integração com API Pública\n")
    try:
        caminho_integracao = os.path.join(base_dir, 'backend', '1-integracao_api_publica', 'main.py')
        modulo_integracao = carregar_modulo(caminho_integracao, 'integracao_main')
        modulo_integracao.principal()
    except Exception as e:
        print(f"Erro ao executar integração: {e}")
    
    # Executar a transformação e validação
    print("\n[INICIANDO] 2. Transformação e Validação de Dados\n")
    try:
        caminho_transformacao = os.path.join(base_dir, 'backend', '2-transformacao_validacao', 'main.py')
        modulo_transformacao = carregar_modulo(caminho_transformacao, 'transformacao_main')
        modulo_transformacao.principal()
    except Exception as e:
        print(f"Erro ao executar transformação: {e}")
    
    print("\n" + "="*70)
    print("SISTEMA FINALIZADO")
    print("="*70)

if __name__ == '__main__':
    main()
