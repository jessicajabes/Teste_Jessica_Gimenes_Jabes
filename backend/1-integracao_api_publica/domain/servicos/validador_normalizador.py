"""Serviço de Domínio para Validação e Normalização de Dados.

Centraliza toda a lógica de validação e normalização de dados
antes do processamento em lotes ou consolidação.
"""

from typing import Dict, Tuple, List
from infraestrutura.logger import get_logger

logger = get_logger('ValidadorNormalizador')


class ValidadorNormalizador:
    """Valida e normaliza dados para processamento."""
    
    @staticmethod
    def normalizar_numero(valor, campo: str = None, contexto: str = None) -> float:
        """Normaliza valores numéricos de string para float.
        
        Converte formato brasileiro (1.234,56) para float (1234.56).
        
        Args:
            valor: Valor a normalizar
            campo: Nome do campo (para logging)
            contexto: Contexto adicional (para logging)
        
        Returns:
            Float normalizado ou None se falhar
        """
        if valor is None:
            return None
        
        try:
            if isinstance(valor, str):
                valor_limpo = valor.strip()
                valor_transformado = valor_limpo.replace('.', '').replace(',', '.')
                numero = float(valor_transformado)
                return numero
            return float(valor)
        except (ValueError, TypeError) as e:
            detalhes = f"campo={campo}" if campo else "campo=desconhecido"
            if contexto:
                detalhes = f"{detalhes}, {contexto}"
            logger.warning(f"Erro na normalização numérica ({detalhes}): valor='{valor}' - {str(e)}")
            return None
    
    @staticmethod
    def limpar_valor(valor) -> str:
        """Remove espaços em branco e retorna None se vazio.
        
        Args:
            valor: Valor a limpar
        
        Returns:
            String limpa ou None se vazio
        """
        if valor is None:
            return None
        
        valor_str = str(valor).strip()
        return valor_str if valor_str else None
    
    @staticmethod
    def validar_campos_obrigatorios(registro: Dict) -> Dict:
        """Valida se os campos obrigatórios estão preenchidos.
        
        Verifica se REG_ANS e DESCRICAO não estão vazios.
        
        Args:
            registro: Dicionário com dados do registro
        
        Returns:
            Dict com:
            - tem_erro: bool indicando se há erro
            - mensagem: str descrevendo os campos vazios
        """
        campos_vazios = []
        
        reg_ans = registro.get('REG_ANS')
        if not reg_ans or not str(reg_ans).strip():
            campos_vazios.append('CNPJ/REG_ANS')
        
        descricao = registro.get('DESCRICAO')
        if not descricao or not str(descricao).strip():
            campos_vazios.append('Razão Social/DESCRICAO')
        
        if campos_vazios:
            return {
                'tem_erro': True,
                'mensagem': f"Campos vazios: {', '.join(campos_vazios)}"
            }
        
        return {
            'tem_erro': False,
            'mensagem': ''
        }
    
    @staticmethod
    def validar_registro(registro: Dict) -> bool:
        """Valida se um registro tem os campos mínimos obrigatórios.
        
        Verifica se existem os campos CD_CONTA_CONTABIL, VL_SALDO_FINAL e VL_SALDO_INICIAL.
        
        Args:
            registro: Dicionário com dados do registro
        
        Returns:
            bool indicando se o registro é válido
        """
        campos_obrigatorios = ['CD_CONTA_CONTABIL', 'VL_SALDO_FINAL', 'VL_SALDO_INICIAL']
        return all(campo in registro for campo in campos_obrigatorios)
    
    @staticmethod
    def calcular_valor_arquivo(dados: List[Dict]) -> float:
        """Calcula o valor total (VL_SALDO_FINAL - VL_SALDO_INICIAL).
        
        Args:
            dados: Lista de dicionários com dados dos registros
        
        Returns:
            Float com valor total calculado
        """
        try:
            valor_total = 0.0
            for registro in dados:
                try:
                    vl_final = registro.get('VL_SALDO_FINAL', 0)
                    vl_inicial = registro.get('VL_SALDO_INICIAL', 0)
                    
                    # Converter para numérico se necessário
                    if isinstance(vl_final, str):
                        vl_final = float(vl_final.replace('.', '').replace(',', '.')) if vl_final else 0
                    if isinstance(vl_inicial, str):
                        vl_inicial = float(vl_inicial.replace('.', '').replace(',', '.')) if vl_inicial else 0
                    
                    valor_total += (float(vl_final or 0) - float(vl_inicial or 0))
                except Exception as e:
                    logger.debug(f"Erro ao calcular valor de registro: {e}")
                    continue
            
            return valor_total
        except Exception as e:
            logger.error(f"Erro ao calcular valor do arquivo: {e}")
            return 0.0
    
    @staticmethod
    def normalizar_valores_dataframe(df, colunas_numericas: List[str] = None) -> dict:
        """Normaliza valores numéricos em um DataFrame.
        
        Args:
            df: DataFrame a normalizar
            colunas_numericas: Lista de nomes de colunas com valores numéricos
        
        Returns:
            Dict com dados normalizados por coluna
        """
        if colunas_numericas is None:
            colunas_numericas = ['vl_saldo_inicial', 'vl_saldo_final', 'valor_trimestre']
        
        resultado = {}
        for col in colunas_numericas:
            if col in df.columns:
                resultado[col] = df[col].apply(
                    lambda x: ValidadorNormalizador.normalizar_numero(x)
                    if x is not None else None
                )
        
        return resultado
