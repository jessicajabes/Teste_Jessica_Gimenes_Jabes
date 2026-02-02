from typing import List, Dict, Callable
from infraestrutura.logger import get_logger

logger = get_logger('ProcessadorEmLotes')

class ProcessadorEmLotes:
    def __init__(self, tamanho_lote: int = 100):
        self.tamanho_lote = tamanho_lote
    
    def processar_em_lotes(
        self,
        registros: List[Dict],
        funcao_inserir: Callable,
        gerenciador_checkpoint,
        arquivo_atual: str,
        registro_inicial: int = 0
    ) -> Dict:
        
        registros_processados = 0
        registros_com_erro = 0
        total_registros = len(registros)
        
        numero_lote = 0
        for i in range(registro_inicial, total_registros, self.tamanho_lote):
            lote = registros[i:i + self.tamanho_lote]
            numero_lote += 1
            
            fim = min(i + self.tamanho_lote, total_registros)
            print(f"    Lote {numero_lote}: {i+1}-{fim} de {total_registros}")
            
            try:
                resultado = funcao_inserir(lote, arquivo_origem=arquivo_atual)

                if resultado is None or resultado is False:
                    registros_com_erro += len(lote)
                    logger.error(f"[FALHA] FALHA ao inserir {len(lote)} registros do arquivo {arquivo_atual} no lote {numero_lote} (resultado=False)")
                    print(f"      [FALHA] ERRO ao inserir {len(lote)} registros (resultado=False)")

                    # AVISO: NÃO atualiza checkpoint para permitir reprocessamento
                else:
                    if isinstance(resultado, int):
                        registros_processados += resultado
                    else:
                        registros_processados += len(lote)

                    #  Atualizar checkpoint quando há sucesso (mesmo com 0 registros válidos)
                    gerenciador_checkpoint.atualizar_checkpoint(
                        arquivo=arquivo_atual,
                        registro=fim,
                        status="em_progresso",
                        registros_processados=registros_processados,
                        registros_erro=registros_com_erro
                    )
            
            except Exception as e:
                logger.error(f"[FALHA] EXCEÇÃO ao processar lote {numero_lote} do arquivo {arquivo_atual}: {str(e)} - Registros não serão contabilizados como erro para permitir reprocessamento")
                print(f"      [FALHA] EXCEÇÃO no lote: {str(e)}")
                # AVISO: NÃO incrementa registros_com_erro para permitir reprocessamento na próxima execução
                # Os registros deste lote permanecerão no checkpoint anterior
        
        return {
            "registros_processados": registros_processados,
            "registros_com_erro": registros_com_erro
        }
