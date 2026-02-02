import json
import os
from datetime import datetime
from typing import Dict
from config import DIRETORIO_CHECKPOINTS
import threading

class GerenciadorCheckpoint:
    _lock = threading.Lock()  # Lock para evitar corrupção de arquivo
    
    def __init__(self, diretorio_checkpoint: str = None):
        self.diretorio = diretorio_checkpoint or DIRETORIO_CHECKPOINTS
        self.arquivo_checkpoint = os.path.join(self.diretorio, "progresso.json")
        os.makedirs(self.diretorio, exist_ok=True)
        self._inicializar_checkpoint()
    
    def _inicializar_checkpoint(self):
        if not os.path.exists(self.arquivo_checkpoint):
            self._salvar({
                "arquivo_atual": None,
                "registro_atual": 0,
                "status": "nao_iniciado",
                "registros_processados": 0,
                "registros_com_erro": 0,
                "trimestres_processados": [],
                "historico": []
            })
    
    def obter_checkpoint(self) -> Dict:
        return self._carregar()
    
    def atualizar_checkpoint(self, arquivo: str, registro: int, status: str, registros_processados: int = None, registros_erro: int = None):
        dados = self._carregar()
        dados["arquivo_atual"] = arquivo
        dados["registro_atual"] = registro
        dados["status"] = status
        dados["timestamp_atualizacao"] = datetime.now().isoformat()
        
        if registros_processados is not None:
            dados["registros_processados"] = registros_processados
        
        if registros_erro is not None:
            dados["registros_com_erro"] = registros_erro
        
        historico = dados.get("historico", [])
        historico.append({
            "arquivo": arquivo,
            "registro": registro,
            "status": status,
            "timestamp": datetime.now().isoformat()
        })
        dados["historico"] = historico[-50:]
        
        self._salvar(dados)
    
    def marcar_arquivo_completo(self, arquivo: str):
        dados = self._carregar()
        dados["arquivo_atual"] = arquivo
        dados["registro_atual"] = 0
        dados["status"] = "arquivo_completo"
        self._salvar(dados)
    
    def marcar_trimestre_processado(self, ano: int, trimestre: int):
        """Registra um trimestre que foi processado com sucesso"""
        dados = self._carregar()
        trimestres = dados.get("trimestres_processados", [])
        
        trimestre_info = {"ano": ano, "trimestre": trimestre, "timestamp": datetime.now().isoformat()}
        
        # Verificar se já não está na lista
        if not any(t["ano"] == ano and t["trimestre"] == trimestre for t in trimestres):
            trimestres.append(trimestre_info)
        
        dados["trimestres_processados"] = trimestres
        self._salvar(dados)
    
    def marcar_processamento_completo(self, total_registros: int, total_erros: int):
        dados = self._carregar()
        dados["status"] = "completo"
        dados["registros_processados"] = total_registros
        dados["registros_com_erro"] = total_erros
        dados["data_conclusao"] = datetime.now().isoformat()
        self._salvar(dados)
    
    def resetar_checkpoint(self):
        self._inicializar_checkpoint()
    
    def _carregar(self) -> Dict:
        with GerenciadorCheckpoint._lock:
            try:
                if not os.path.exists(self.arquivo_checkpoint):
                    self._inicializar_checkpoint()
                with open(self.arquivo_checkpoint, 'r') as f:
                    return json.load(f)
            except (json.JSONDecodeError, Exception) as e:
                # Se o JSON está corrompido, reinicializar
                print(f"[AVISO] Arquivo de checkpoint corrompido ({type(e).__name__}), reinicializando...")
                try:
                    # Deletar arquivo corrompido antes de reinicializar
                    if os.path.exists(self.arquivo_checkpoint):
                        os.remove(self.arquivo_checkpoint)
                except Exception as del_error:
                    print(f"[AVISO] Não foi possível deletar arquivo corrompido: {del_error}")
                
                self._inicializar_checkpoint()
                # Tentar ler novamente com retry
                max_retries = 3
                for attempt in range(max_retries):
                    try:
                        with open(self.arquivo_checkpoint, 'r') as f:
                            return json.load(f)
                    except Exception as retry_error:
                        if attempt < max_retries - 1:
                            print(f"[AVISO] Tentativa {attempt + 1} falhou, retentando...")
                            self._inicializar_checkpoint()
                        else:
                            print(f"[ERRO] Falha ao carregar checkpoint após {max_retries} tentativas: {retry_error}")
                            raise
    
    def _salvar(self, dados: Dict):
        with GerenciadorCheckpoint._lock:
            try:
                os.makedirs(self.diretorio, exist_ok=True, mode=0o777)
                # Salvar em arquivo temporário primeiro
                arquivo_temp = self.arquivo_checkpoint + '.tmp'
                with open(arquivo_temp, 'w', encoding='utf-8') as f:
                    json.dump(dados, f, indent=2, ensure_ascii=False)
                # Renomear para arquivo final (atômico em Linux)
                os.replace(arquivo_temp, self.arquivo_checkpoint)
            except Exception as e:
                print(f"AVISO: Erro ao salvar checkpoint: {e}")
                # Tentar salvar em local alternativo
                try:
                    arquivo_alternativo = '/tmp/checkpoint_progresso.json'
                    with open(arquivo_alternativo, 'w', encoding='utf-8') as f:
                        json.dump(dados, f, indent=2, ensure_ascii=False)
                except:
                    pass  # Silent fail, o checkpoint será perdido mas o processamento continua
    
    def exibir_status(self):
        dados = self._carregar()
        print("\n" + "="*60)
        print("STATUS DO PROCESSAMENTO")
        print("="*60)
        print(f"Status: {dados['status']}")
        print(f"Arquivo Atual: {dados.get('arquivo_atual', 'Nenhum')}")
        print(f"Registro Atual: {dados.get('registro_atual', 0)}")
        print(f"Registros Processados: {dados.get('registros_processados', 0)}")
        print(f"Registros com Erro: {dados.get('registros_com_erro', 0)}")
        
        trimestres = dados.get('trimestres_processados', [])
        if trimestres:
            print(f"\nTrimestres Processados:")
            for t in trimestres:
                print(f"  - {t['trimestre']}T{t['ano']}")
        else:
            print(f"Trimestres Processados: Nenhum")
        
        if dados.get('data_conclusao'):
            print(f"\nConcluído em: {dados['data_conclusao']}")
        print("="*60 + "\n")
