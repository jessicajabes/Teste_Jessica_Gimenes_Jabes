"""Serviço de Domínio para Enriquecimento de Dados com Operadoras."""

import pandas as pd
from typing import Dict


class EnriquecedorOperadoras:
    """Enriquece dados com informações de operadoras."""
    
    @staticmethod
    def criar_mapa_por_registro_ans(operadoras: pd.DataFrame, logger=None) -> Dict[str, Dict]:
        """Cria mapa indexado por REG_ANS para enriquecimento.
        
        Args:
            operadoras: DataFrame com dados de operadoras
            logger: Logger opcional para registrar erros
            
        Returns:
            Dict mapeando reg_ans → {tipo, modalidade, uf}
        """
        mapa = {}
        
        if operadoras.empty:
            return mapa
        
        # Agrupar por reg_ans para detectar duplicidades
        grupos = operadoras.groupby("reg_ans")
        
        for reg_ans, grupo in grupos:
            reg_ans_str = str(reg_ans).strip()
            
            # Filtrar registros ativos
            ativos = grupo[grupo["status"].str.upper() == "ATIVA"]
            
            if ativos.empty:
                # Nenhum ativo - procurar canceladas
                canceladas = grupo[grupo["status"].str.upper() == "CANCELADA"]
                
                if canceladas.empty:
                    # Nenhuma cancelada - registro não encontrado
                    mapa[reg_ans_str] = {
                        "tipo": "N/L",
                        "modalidade": "N/L",
                        "uf": "N/L",
                    }
                elif len(canceladas) == 1:
                    # Uma cancelada - usa seus dados
                    cancelada = canceladas.iloc[0]
                    mapa[reg_ans_str] = {
                        "tipo": "CANCELADA",
                        "modalidade": str(cancelada.get("modalidade", "N/L")).strip(),
                        "uf": str(cancelada.get("uf", "N/L")).strip(),
                    }
                else:
                    # Múltiplas canceladas - marca como duplicidade
                    if logger:
                        logger.error(
                            f"REGISTROANS com múltiplas operadoras canceladas: {reg_ans_str} "
                            f"({len(canceladas)} registros encontrados)"
                        )
                    mapa[reg_ans_str] = {
                        "tipo": "DUPLICIDADE",
                        "modalidade": None,
                        "uf": None,
                    }
            elif len(ativos) == 1:
                # Um ativo - usa seus dados
                ativo = ativos.iloc[0]
                mapa[reg_ans_str] = {
                    "tipo": "ATIVO",
                    "modalidade": str(ativo.get("modalidade", "N/L")).strip(),
                    "uf": str(ativo.get("uf", "N/L")).strip(),
                }
            else:
                # Múltiplos ativos - marca como duplicidade
                if logger:
                    logger.error(
                        f"REGISTROANS com múltiplas operadoras ativas: {reg_ans_str} "
                        f"({len(ativos)} registros encontrados)"
                    )
                mapa[reg_ans_str] = {
                    "tipo": "DUPLICIDADE",
                    "modalidade": None,
                    "uf": None,
                }
        
        return mapa

    @staticmethod
    def enriquecer_com_modalidade_uf(
        df: pd.DataFrame, 
        mapa_reg_ans: Dict[str, Dict],
        logger=None,
        nome_base: str = "dataset"
    ) -> pd.DataFrame:
        """Enriquece DataFrame com MODALIDADE e UF baseado em REGISTROANS.
        
        Args:
            df: DataFrame a enriquecer
            mapa_reg_ans: Mapa de reg_ans → info
            logger: Logger opcional para registrar avisos
            nome_base: Nome do dataset para logs
            
        Returns:
            DataFrame enriquecido com colunas MODALIDADE e UF
        """
        df = df.copy()
        
        # Garantir que REGISTROANS exista
        if "REGISTROANS" not in df.columns:
            df["REGISTROANS"] = "N/L"

        modalidades = []
        ufs = []

        for idx, row in df.iterrows():
            reg_ans = str(row["REGISTROANS"]).strip() if pd.notna(row["REGISTROANS"]) else None
            
            if not reg_ans or reg_ans in ["", "N/L", "nan", "None"]:
                modalidades.append("N/L")
                ufs.append("N/L")
                continue

            info = mapa_reg_ans.get(reg_ans)
            
            if info is None:
                if logger:
                    logger.warning(
                        f"{nome_base}: REGISTROANS não encontrado no cadastro "
                        f"na linha {idx + 1} ({reg_ans})"
                    )
                modalidades.append("N/L")
                ufs.append("N/L")
            elif info["tipo"] == "DUPLICIDADE":
                if logger:
                    logger.error(
                        f"{nome_base}: REGISTROANS com duplicidade ativa "
                        f"na linha {idx + 1} ({reg_ans})"
                    )
                modalidades.append("REG. DUPLICIDADE")
                ufs.append("REG. DUPLICIDADE")
            else:
                modalidades.append(info["modalidade"] or "N/L")
                ufs.append(info["uf"] or "N/L")

        df["MODALIDADE"] = modalidades
        df["UF"] = ufs

        return df
