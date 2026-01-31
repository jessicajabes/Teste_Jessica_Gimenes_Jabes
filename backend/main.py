import sys
import os
import subprocess

def mostrar_menu():
    """Exibe menu de seleção de módulos"""
    print("="*70)
    print(" "*15 + "SISTEMA DE PROCESSAMENTO DE DADOS - ANS")
    print("="*70)
    print("\nEscolha qual módulo deseja executar:\n")
    print("  1 - Integração API Pública")
    print("     (Download e consolidação de dados da ANS)")
    print()
    print("  2 - Transformação e Validação")
    print("     (Processamento e validação dos dados consolidados)")
    print()
    print("  0 - Sair")
    print("\n" + "="*70)

def executar_modulo(opcao):
    """Executa o módulo selecionado"""
    modulos = {
        '1': {
            'nome': 'Integração API Pública',
            'caminho': os.path.join(os.path.dirname(__file__), '1-integracao_api_publica', 'main.py'),
            'diretorio': os.path.join(os.path.dirname(__file__), '1-integracao_api_publica')
        },
        '2': {
            'nome': 'Transformação e Validação',
            'caminho': os.path.join(os.path.dirname(__file__), '2-transformacao_validacao', 'main.py'),
            'diretorio': os.path.join(os.path.dirname(__file__), '2-transformacao_validacao')
        }
    }
    
    if opcao not in modulos:
        print("\n[ERRO] Opção inválida!")
        return False
    
    modulo = modulos[opcao]
    
    if not os.path.exists(modulo['caminho']):
        print(f"\n[ERRO] Arquivo não encontrado: {modulo['caminho']}")
        return False
    
    print(f"\n[INICIANDO] {modulo['nome']}")
    print("-" * 70)
    
    try:
        # Executar o main.py do módulo selecionado
        resultado = subprocess.run(
            [sys.executable, modulo['caminho']],
            cwd=modulo['diretorio'],
            check=False
        )
        
        if resultado.returncode == 0:
            print("-" * 70)
            print(f"[OK] {modulo['nome']} concluído com sucesso!")
        else:
            print("-" * 70)
            print(f"[AVISO] {modulo['nome']} finalizou com código: {resultado.returncode}")
        
        return True
    except Exception as e:
        print(f"\n[ERRO] Erro ao executar {modulo['nome']}: {e}")
        return False

def main():
    """Função principal - Menu interativo"""
    while True:
        mostrar_menu()
        opcao = input("\nDigite sua escolha (0-2): ").strip()
        
        if opcao == '0':
            print("\n👋 Encerrando sistema...")
            break
        
        if opcao in ['1', '2']:
            executar_modulo(opcao)
            input("\nPressione ENTER para voltar ao menu...")
            print("\n")
        else:
            print("\n[ERRO] Opção inválida! Tente novamente.\n")

if __name__ == '__main__':
    main()
