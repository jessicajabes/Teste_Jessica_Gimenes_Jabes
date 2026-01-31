from dataclasses import dataclass
from typing import List

@dataclass
class Trimestre:
    ano: int
    numero: int
    
    def __str__(self) -> str:
        return f"{self.ano}/{self.numero}T"
    
    def para_string_api(self) -> str:
        return f"{self.numero}T"

@dataclass
class Arquivo:
    nome: str
    caminho: str
    trimestre: Trimestre
    
    @property
    def nome_base(self) -> str:
        import os
        return os.path.basename(self.nome)
