"""
Entidades de domínio para transformação e validação
"""

from dataclasses import dataclass
from datetime import date
from typing import Optional

@dataclass
class DemonstracaoContabil:
    """Representa um registro de demonstração contábil"""
    reg_ans: str
    cd_conta_contabil: str
    descricao: str
    vl_saldo_inicial: float
    vl_saldo_final: float
    trimestre: int
    ano: int
    data: Optional[date] = None
    
    def validar(self) -> tuple[bool, str]:
        """Valida os dados do registro"""
        if not self.reg_ans:
            return False, "REG_ANS não pode estar vazio"
        
        if not self.cd_conta_contabil:
            return False, "CD_CONTA_CONTABIL não pode estar vazio"
        
        if self.trimestre < 1 or self.trimestre > 4:
            return False, f"Trimestre inválido: {self.trimestre}"
        
        if self.ano < 1900 or self.ano > 2100:
            return False, f"Ano inválido: {self.ano}"
        
        return True, "OK"

@dataclass
class ResultadoImportacao:
    """Representa o resultado de uma importação"""
    total_registros: int
    registros_importados: int
    registros_com_erro: int
    erros: list[str]
    tempo_execucao: float
