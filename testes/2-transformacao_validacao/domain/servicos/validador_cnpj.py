"""Serviço de Domínio para Validação de CNPJ."""

import re
from typing import Tuple, Optional


class ValidadorCNPJ:
    """Valida CNPJs conforme regras da Receita Federal."""
    
    @staticmethod
    def validar(valor: str) -> Tuple[Optional[str], bool, bool]:
        """Valida CNPJ e retorna (cnpj_limpo, formato_ok, digito_verificador_ok).
        
        Args:
            valor: CNPJ a validar
            
        Returns:
            Tuple com:
            - cnpj_limpo: CNPJ apenas com dígitos (14 chars) ou None
            - formato_ok: Se formato está correto
            - digito_verificador_ok: Se dígitos verificadores estão corretos
        """
        if valor is None:
            return None, False, False

        texto = str(valor).strip()
        
        # Verifica formato
        formato_ok = bool(
            re.match(r"^\d{2}\.\d{3}\.\d{3}/\d{4}-\d{2}$", texto) or 
            re.match(r"^\d{14}$", texto)
        )
        
        digitos = re.sub(r"\D", "", texto)

        # Completa com zeros à esquerda até 14 dígitos (1 a 13)
        if 1 <= len(digitos) <= 13:
            digitos = digitos.zfill(14)

        # Se não tem 14 dígitos, é inválido
        if len(digitos) != 14:
            return None, formato_ok, False

        # Verifica se todos os dígitos são iguais (00000000000000, 11111111111111, etc)
        if len(set(digitos)) == 1:
            return digitos, formato_ok, False

        # Calcula dígitos verificadores
        dv_ok = ValidadorCNPJ._verificar_digitos(digitos)

        return digitos, formato_ok, dv_ok

    @staticmethod
    def _verificar_digitos(cnpj: str) -> bool:
        """Verifica se os dígitos verificadores do CNPJ estão corretos."""
        if len(cnpj) != 14:
            return False

        # Primeiro dígito verificador
        pesos1 = [5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2]
        soma1 = sum(int(cnpj[i]) * pesos1[i] for i in range(12))
        resto1 = soma1 % 11
        dv1 = 0 if resto1 < 2 else 11 - resto1

        if dv1 != int(cnpj[12]):
            return False

        # Segundo dígito verificador
        pesos2 = [6, 5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2]
        soma2 = sum(int(cnpj[i]) * pesos2[i] for i in range(13))
        resto2 = soma2 % 11
        dv2 = 0 if resto2 < 2 else 11 - resto2

        return dv2 == int(cnpj[13])

    @staticmethod
    def limpar(valor: str) -> Optional[str]:
        """Remove formatação e retorna apenas os dígitos do CNPJ.
        
        Args:
            valor: CNPJ formatado ou não
            
        Returns:
            CNPJ com 14 dígitos ou None se inválido
        """
        if valor is None:
            return None
        
        digitos = re.sub(r"\D", "", str(valor))
        return digitos if len(digitos) == 14 else None
