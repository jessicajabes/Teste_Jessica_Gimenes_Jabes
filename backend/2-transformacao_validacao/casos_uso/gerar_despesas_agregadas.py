"""
Processamento de Despesas Agregadas

Valida arquivos de despesas, registra inconsistências em log,
enriquece dados via tabela de operadoras e gera agregações.
"""

import os
import re
import io
import zipfile
import logging
from datetime import datetime
from typing import Dict, Optional, Tuple

import pandas as pd
from sqlalchemy import create_engine

from config import DATABASE_URL, DIRETORIO_CONSOLIDADOS, DIRETORIO_TRANSFORMACAO


class GerarDespesasAgregadas:
    def __init__(self, diretorio_dados: str = DIRETORIO_CONSOLIDADOS):
        self.diretorio_dados = diretorio_dados
        self.diretorio_saida = DIRETORIO_TRANSFORMACAO
        os.makedirs(self.diretorio_saida, exist_ok=True)

        self.zip_path = self._localizar_zip()
        self.log_arquivo_zip_nome = None
        self.log_file_path = self._preparar_log_file()
        self.logger = self._configurar_logger(self.log_file_path)

        self.arquivo_sinistros_sem_deducoes = "sinistro_sem_deducoes.csv"
        self.arquivo_sinistros_c_deducoes = "consolidado_despesas_sinistros_c_deducoes.csv"

        self.arquivo_saida_sem_deducoes = os.path.join(self.diretorio_saida, "despesas_agregadas.csv")
        self.arquivo_saida_c_deducoes = os.path.join(self.diretorio_saida, "despesas_agregadas_c_deducoes.csv")

    def executar(self):
        print("=" * 60)
        print("VALIDAÇÃO E AGREGAÇÃO DE DESPESAS")
        print("=" * 60)

        operadoras = self._carregar_operadoras()

        df_sem = self._carregar_despesas(self.arquivo_sinistros_sem_deducoes)
        if df_sem is not None:
            df_sem = self._validar_e_enriquecer(df_sem, operadoras, "sinistro_sem_deducoes")
            agreg_sem = self._agregar(df_sem)
            if agreg_sem is not None:
                self._salvar_agregado(agreg_sem, self.arquivo_saida_sem_deducoes)

        df_com = self._carregar_despesas(self.arquivo_sinistros_c_deducoes)
        if df_com is not None:
            df_com = self._validar_e_enriquecer(df_com, operadoras, "consolidado_despesas_sinistros_c_deducoes")
            agreg_com = self._agregar(df_com)
            if agreg_com is not None:
                self._salvar_agregado(agreg_com, self.arquivo_saida_c_deducoes)

        self._atualizar_log_no_zip()
        self._criar_zip_transformacao()

        print("=" * 60)
        print("PROCESSO CONCLUÍDO")
        print("=" * 60)

    def _localizar_zip(self) -> Optional[str]:
        caminho_zip = os.path.join(self.diretorio_dados, "consolidado_despesas.zip")
        if os.path.exists(caminho_zip):
            return caminho_zip

        if os.path.isdir(self.diretorio_dados):
            for nome in os.listdir(self.diretorio_dados):
                if nome.lower().endswith(".zip") and "consolidado_despesas" in nome.lower():
                    return os.path.join(self.diretorio_dados, nome)
        return None

    def _preparar_log_file(self) -> str:
        base_integracao = os.path.dirname(self.diretorio_saida)
        log_dir = os.path.join(base_integracao, "logs")
        os.makedirs(log_dir, exist_ok=True)

        if not self.zip_path:
            return os.path.join(log_dir, "validacao.log")

        try:
            with zipfile.ZipFile(self.zip_path, "r") as zipf:
                nome_log = self._encontrar_log_zip(zipf.namelist())
                if nome_log:
                    self.log_arquivo_zip_nome = nome_log
                    destino = os.path.join(log_dir, os.path.basename(nome_log))
                    if not os.path.exists(destino):
                        with zipf.open(nome_log) as origem, open(destino, "wb") as saida:
                            saida.write(origem.read())
                    return destino
        except Exception:
            pass

        return os.path.join(log_dir, "validacao.log")

    @staticmethod
    def _encontrar_log_zip(nomes: list) -> Optional[str]:
        candidatos = []
        for nome in nomes:
            nome_lower = nome.lower()
            if nome_lower.endswith(".log") or ("log" in nome_lower and nome_lower.endswith(".txt")):
                candidatos.append(nome)
        return candidatos[0] if candidatos else None

    @staticmethod
    def _configurar_logger(log_file_path: str) -> logging.Logger:
        logger = logging.getLogger("TransformacaoValidacao")
        logger.setLevel(logging.DEBUG)

        for handler in logger.handlers[:]:
            logger.removeHandler(handler)

        formato = logging.Formatter(
            "%(asctime)s | %(levelname)-8s | %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )

        fh = logging.FileHandler(log_file_path, encoding="utf-8", mode="a")
        fh.setLevel(logging.DEBUG)
        fh.setFormatter(formato)
        logger.addHandler(fh)

        ch = logging.StreamHandler()
        ch.setLevel(logging.INFO)
        ch.setFormatter(formato)
        logger.addHandler(ch)

        return logger

    def _carregar_despesas(self, nome_arquivo) -> Optional[pd.DataFrame]:
        if isinstance(nome_arquivo, list):
            for nome in nome_arquivo:
                df = self._carregar_despesas(nome)
                if df is not None:
                    return df
            return None

        if self.zip_path:
            df = self._ler_csv_do_zip(self.zip_path, nome_arquivo)
            if df is not None:
                print(f"✓ Carregado do ZIP: {nome_arquivo} ({len(df)} registros)")
                return df

        caminho = os.path.join(self.diretorio_dados, nome_arquivo)
        if os.path.exists(caminho):
            try:
                df = pd.read_csv(caminho, sep=";", encoding="utf-8-sig")
                print(f"✓ Carregado: {nome_arquivo} ({len(df)} registros)")
                return df
            except Exception as e:
                self.logger.error(f"Erro ao ler {nome_arquivo}: {e}")
                return None

        self.logger.error(f"Arquivo não encontrado: {nome_arquivo}")
        return None

    @staticmethod
    def _ler_csv_do_zip(caminho_zip: str, nome_arquivo: str) -> Optional[pd.DataFrame]:
        try:
            with zipfile.ZipFile(caminho_zip, "r") as zipf:
                if nome_arquivo not in zipf.namelist():
                    return None
                with zipf.open(nome_arquivo) as arquivo:
                    conteudo = arquivo.read()
                    return pd.read_csv(io.BytesIO(conteudo), sep=";", encoding="utf-8-sig")
        except Exception:
            return None

    def _carregar_operadoras(self) -> pd.DataFrame:
        try:
            url = DATABASE_URL
            if url.startswith("postgresql://"):
                url = url.replace("postgresql://", "postgresql+psycopg2://", 1)
            engine = create_engine(url, echo=False)
            df = pd.read_sql_query(
                "SELECT cnpj, reg_ans, modalidade, uf, status FROM operadoras",
                engine,
            )
            if df.empty:
                self.logger.warning("Tabela de operadoras vazia")
            df.columns = df.columns.str.lower().str.strip()
            df["cnpj_limpo"] = df["cnpj"].astype(str).apply(self._limpar_cnpj)
            df["status_upper"] = df["status"].astype(str).str.upper().str.strip()
            return df
        except Exception as e:
            self.logger.error(f"Erro ao carregar operadoras: {e}")
            return pd.DataFrame(columns=["cnpj", "reg_ans", "modalidade", "uf", "status", "cnpj_limpo", "status_upper"])

    def _validar_e_enriquecer(self, df: pd.DataFrame, operadoras: pd.DataFrame, nome_base: str) -> pd.DataFrame:
        df = df.copy()
        self._normalizar_colunas(df)

        colunas_obrigatorias = ["CNPJ", "RAZAO_SOCIAL", "VALOR_DE_DESPESAS", "TRIMESTRE", "ANO"]
        faltantes = [c for c in colunas_obrigatorias if c not in df.columns]
        if faltantes:
            self.logger.error(f"{nome_base}: colunas obrigatórias ausentes: {', '.join(faltantes)}")
            return pd.DataFrame()

        razao_series = df["RAZAO_SOCIAL"]
        mascara_razao_vazia = (
            razao_series.isna()
            | razao_series.astype(str).str.strip().str.lower().isin(["", "nan", "none"])
        )
        if mascara_razao_vazia.any():
            for idx in df[mascara_razao_vazia].index:
                self.logger.error(f"{nome_base}: RAZAO SOCIAL vazia na linha {idx + 1}")
            df.loc[mascara_razao_vazia, "RAZAO_SOCIAL"] = "N/L"

        df["VALOR_NUM"] = df["VALOR_DE_DESPESAS"].apply(self._parse_valor)
        mascara_valor_invalido = df["VALOR_NUM"].isna()
        if mascara_valor_invalido.any():
            for idx in df[mascara_valor_invalido].index:
                self.logger.error(f"{nome_base}: valor numérico inválido na linha {idx + 1}")

        # Verificar valores negativos (exceto deduções que começam com "-" ou "(-)")
        if "DESCRICAO" in df.columns:
            descricao_series = df["DESCRICAO"].astype(str).str.strip()
            eh_deducao = descricao_series.str.startswith("-") | descricao_series.str.startswith("(-)")
            mascara_negativo = df["VALOR_NUM"].notna() & (df["VALOR_NUM"] < 0) & (~eh_deducao)
        else:
            mascara_negativo = df["VALOR_NUM"].notna() & (df["VALOR_NUM"] < 0)
        
        if mascara_negativo.any():
            for idx in df[mascara_negativo].index:
                self.logger.warning(f"{nome_base}: valor de despesa negativo na linha {idx + 1}")

        # Verificar se deduções têm valor positivo (deduções deveriam ser negativas)
        if "DESCRICAO" in df.columns:
            mascara_deducao_positiva = df["VALOR_NUM"].notna() & (df["VALOR_NUM"] > 0) & eh_deducao
            if mascara_deducao_positiva.any():
                for idx in df[mascara_deducao_positiva].index:
                    self.logger.warning(f"{nome_base}: dedução com valor positivo na linha {idx + 1}")

        mascara_zero = df["VALOR_NUM"].notna() & (df["VALOR_NUM"] == 0)
        if mascara_zero.any():
            for idx in df[mascara_zero].index:
                self.logger.warning(f"{nome_base}: valor de despesa igual a zero na linha {idx + 1}")

        df["CNPJ"] = df["CNPJ"].astype(str)
        cnpj_limp, cnpj_format_ok, cnpj_dv_ok = [], [], []
        for valor in df["CNPJ"]:
            limpo, formato_ok, dv_ok = self._validar_cnpj(valor)
            cnpj_limp.append(limpo)
            cnpj_format_ok.append(formato_ok)
            cnpj_dv_ok.append(dv_ok)

        df["CNPJ_LIMPO"] = cnpj_limp
        df["CNPJ_FORMATO_OK"] = cnpj_format_ok
        df["CNPJ_DV_OK"] = cnpj_dv_ok

        for idx, row in df.iterrows():
            cnpj_original = str(row['CNPJ']).strip()
            digitos_originais = re.sub(r"\D", "", cnpj_original)
            tamanho_original = len(digitos_originais)
            
            # Não gerar erro/aviso quando apenas normalizou (1-13 dígitos)
            if tamanho_original == 0 or tamanho_original >= 14:
                if not row["CNPJ_FORMATO_OK"]:
                    self.logger.error(f"{nome_base}: CNPJ com formato inválido na linha {idx + 1} ({row['CNPJ']})")
                if not row["CNPJ_DV_OK"]:
                    self.logger.error(f"{nome_base}: CNPJ com dígitos verificadores inválidos na linha {idx + 1} ({row['CNPJ']})")

        # Garantir que REGISTROANS exista
        if "REGISTROANS" not in df.columns:
            df["REGISTROANS"] = "N/L"

        # Buscar MODALIDADE e UF pela REGISTROANS
        mapa_reg_ans = self._mapa_por_registro_ans(operadoras)
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
                self.logger.warning(f"{nome_base}: REGISTROANS não encontrado no cadastro na linha {idx + 1} ({reg_ans})")
                modalidades.append("N/L")
                ufs.append("N/L")
            elif info["tipo"] == "DUPLICIDADE":
                self.logger.error(f"{nome_base}: REGISTROANS com duplicidade ativa na linha {idx + 1} ({reg_ans})")
                modalidades.append("REG. DUPLICIDADE")
                ufs.append("REG. DUPLICIDADE")
            else:
                modalidades.append(info["modalidade"] or "N/L")
                ufs.append(info["uf"] or "N/L")

        df["MODALIDADE"] = modalidades
        df["UF"] = ufs

        return df

    @staticmethod
    def _mapa_por_registro_ans(operadoras: pd.DataFrame) -> Dict[str, Dict]:
        mapa = {}
        if operadoras.empty:
            return mapa

        # Limpar reg_ans para usar como chave
        operadoras_clean = operadoras.copy()
        operadoras_clean["reg_ans_limpo"] = operadoras_clean["reg_ans"].astype(str).str.strip()

        for reg_ans, grupo in operadoras_clean.groupby("reg_ans_limpo"):
            if not reg_ans or reg_ans in ["", "nan", "None"]:
                continue

            grupo_valido = grupo.dropna(subset=["reg_ans_limpo"])
            if grupo_valido.empty:
                continue

            ativos = grupo_valido[grupo_valido["status_upper"] == "ATIVO"]
            
            # Se só tem 1 registro no total, usa ele
            if len(grupo_valido) == 1:
                registro = grupo_valido.iloc[0]
                mapa[reg_ans] = {
                    "tipo": "OK",
                    "modalidade": str(registro.get("modalidade", "")).strip(),
                    "uf": str(registro.get("uf", "")).strip(),
                }
            # Se só tem 1 ativo, usa ele
            elif len(ativos) == 1:
                registro = ativos.iloc[0]
                mapa[reg_ans] = {
                    "tipo": "OK",
                    "modalidade": str(registro.get("modalidade", "")).strip(),
                    "uf": str(registro.get("uf", "")).strip(),
                }
            # Se tem mais de 1 ativo, marca duplicidade
            else:
                mapa[reg_ans] = {"tipo": "DUPLICIDADE"}

        return mapa

    @staticmethod
    def _normalizar_colunas(df: pd.DataFrame):
        mapping = {}
        for col in df.columns:
            chave = re.sub(r"[\s_]+", "", col.strip().upper())
            if chave == "CNPJ":
                mapping[col] = "CNPJ"
            elif chave in ("RAZAOSOCIAL", "RAZAOSOCIALOPERADORA"):
                mapping[col] = "RAZAO_SOCIAL"
            elif chave in ("VALORDEDESPESAS", "VALORTRIMESTRE"):
                mapping[col] = "VALOR_DE_DESPESAS"
            elif chave == "TRIMESTRE":
                mapping[col] = "TRIMESTRE"
            elif chave == "ANO":
                mapping[col] = "ANO"
            elif chave in ("REGISTROANS", "REGANS"):
                mapping[col] = "REGISTROANS"
            elif chave == "MODALIDADE":
                mapping[col] = "MODALIDADE"
            elif chave == "UF":
                mapping[col] = "UF"
        df.rename(columns=mapping, inplace=True)

    @staticmethod
    def _parse_valor(valor) -> Optional[float]:
        if pd.isna(valor):
            return None
        try:
            if isinstance(valor, str):
                valor = valor.strip()
                if valor == "":
                    return None
                valor = valor.replace(".", "").replace(",", ".")
                return float(valor)
            return float(valor)
        except Exception:
            return None

    @staticmethod
    def _limpar_cnpj(valor: str) -> Optional[str]:
        if valor is None:
            return None
        digitos = re.sub(r"\D", "", str(valor))
        return digitos if len(digitos) == 14 else None

    def _validar_cnpj(self, valor: str) -> Tuple[Optional[str], bool, bool]:
        if valor is None:
            return None, False, False

        texto = str(valor).strip()
        formato_ok = bool(re.match(r"^\d{2}\.\d{3}\.\d{3}/\d{4}-\d{2}$", texto) or re.match(r"^\d{14}$", texto))
        digitos = re.sub(r"\D", "", texto)

        # Completa com zeros à esquerda até 14 dígitos (1 a 13)
        if 1 <= len(digitos) <= 13:
            digitos = digitos.zfill(14)
            formato_ok = True

        if len(digitos) != 14:
            return None, formato_ok, False

        if len(set(digitos)) == 1:
            return digitos, formato_ok, False

        def calcular_dv(base, pesos):
            soma = sum(int(num) * peso for num, peso in zip(base, pesos))
            resto = soma % 11
            return "0" if resto < 2 else str(11 - resto)

        pesos_1 = [5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2]
        pesos_2 = [6, 5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2]

        dv1 = calcular_dv(digitos[:12], pesos_1)
        dv2 = calcular_dv(digitos[:12] + dv1, pesos_2)

        dv_ok = digitos[-2:] == dv1 + dv2
        return digitos, formato_ok, dv_ok

    def _agregar(self, df: pd.DataFrame) -> Optional[pd.DataFrame]:
        if df.empty:
            return None

        df_valid = df[df["VALOR_NUM"].notna()].copy()
        if df_valid.empty:
            return None

        df_valid["UF"] = df_valid["UF"].fillna("N/L")

        base = df_valid.groupby(["RAZAO_SOCIAL", "UF"], dropna=False).agg(
            total_despesas=("VALOR_NUM", "sum"),
            qtd_registros=("VALOR_NUM", "size"),
            qtd_trimestres=("TRIMESTRE", "nunique"),
            qtd_anos=("ANO", "nunique"),
        ).reset_index()

        por_trimestre = df_valid.groupby(["RAZAO_SOCIAL", "UF", "TRIMESTRE"], dropna=False)["VALOR_NUM"].sum().reset_index()
        stats = por_trimestre.groupby(["RAZAO_SOCIAL", "UF"], dropna=False)["VALOR_NUM"].agg(
            media_despesas_trimestre="mean",
            desvio_padrao_despesas="std",
        ).reset_index()

        resultado = base.merge(stats, on=["RAZAO_SOCIAL", "UF"], how="left")
        resultado["desvio_padrao_despesas"] = resultado["desvio_padrao_despesas"].fillna(0)

        resultado = resultado.sort_values("total_despesas", ascending=False)

        resultado.rename(columns={
            "RAZAO_SOCIAL": "razao_social",
            "UF": "uf",
        }, inplace=True)

        return resultado[[
            "razao_social",
            "uf",
            "total_despesas",
            "media_despesas_trimestre",
            "desvio_padrao_despesas",
            "qtd_registros",
            "qtd_trimestres",
            "qtd_anos",
        ]]

    @staticmethod
    def _salvar_agregado(df: pd.DataFrame, caminho_saida: str):
        df.to_csv(caminho_saida, index=False, encoding="utf-8-sig", sep=";")
        # Não exibe mensagem aqui, será exibida após adicionar ao ZIP

    def _atualizar_log_no_zip(self):
        if not self.zip_path or not self.log_arquivo_zip_nome or not os.path.exists(self.log_file_path):
            return

        temp_zip = self.zip_path + ".tmp"
        try:
            with zipfile.ZipFile(self.zip_path, "r") as zipf_old, zipfile.ZipFile(temp_zip, "w", zipfile.ZIP_DEFLATED) as zipf_new:
                # Copiar todos os arquivos do ZIP antigo, exceto o log e os arquivos agregados antigos
                for item in zipf_old.infolist():
                    if item.filename == self.log_arquivo_zip_nome:
                        continue
                    if item.filename in ["despesas_agregadas.csv", "despesas_agregadas_c_deducoes.csv"]:
                        continue
                    zipf_new.writestr(item, zipf_old.read(item.filename))

                # Adicionar o log atualizado
                zipf_new.write(self.log_file_path, self.log_arquivo_zip_nome)
                
                # Adicionar os arquivos agregados ao ZIP
                if os.path.exists(self.arquivo_saida_sem_deducoes):
                    zipf_new.write(self.arquivo_saida_sem_deducoes, "despesas_agregadas.csv")
                    
                if os.path.exists(self.arquivo_saida_c_deducoes):
                    zipf_new.write(self.arquivo_saida_c_deducoes, "despesas_agregadas_c_deducoes.csv")

            os.replace(temp_zip, self.zip_path)
            
            print(f"✓ ZIP atualizado: {self.zip_path}")
            print(f"✓ Arquivo gerado: despesas_agregadas.csv ({self.arquivo_saida_sem_deducoes})")
            print(f"✓ Arquivo gerado: despesas_agregadas_c_deducoes.csv ({self.arquivo_saida_c_deducoes})")
            
        except Exception as e:
            self.logger.error(f"Erro ao atualizar log no ZIP: {e}")
            try:
                if os.path.exists(temp_zip):
                    os.remove(temp_zip)
            except Exception:
                pass

    def _criar_zip_transformacao(self):
        arquivos_csv = [
            self.arquivo_saida_sem_deducoes,
            self.arquivo_saida_c_deducoes,
        ]

        arquivos_existentes = [c for c in arquivos_csv if os.path.exists(c)]
        if not arquivos_existentes:
            return

        log_dir = os.path.dirname(self.log_file_path)
        arquivos_log = []
        if os.path.isdir(log_dir):
            arquivos_log = [
                os.path.join(log_dir, nome)
                for nome in os.listdir(log_dir)
                if nome.lower().endswith(".log")
            ]

        caminho_zip = os.path.join(self.diretorio_saida, "despesas_agregadas_com_logs.zip")
        try:
            with zipfile.ZipFile(caminho_zip, "w", zipfile.ZIP_DEFLATED) as zipf:
                for caminho in arquivos_existentes:
                    zipf.write(caminho, os.path.basename(caminho))

                for caminho in arquivos_log:
                    if os.path.exists(caminho):
                        zipf.write(caminho, os.path.join("logs", os.path.basename(caminho)))

            print(f"✓ ZIP gerado: {caminho_zip}")
        except Exception as e:
            self.logger.error(f"Erro ao criar ZIP de transformação: {e}")
