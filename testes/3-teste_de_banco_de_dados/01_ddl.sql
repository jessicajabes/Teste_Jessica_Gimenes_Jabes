-- DDL das tabelas do módulo 3

-- Tabela de Operadoras
DROP TABLE IF EXISTS operadoras CASCADE;
CREATE TABLE operadoras (
    reg_ans             VARCHAR(20) PRIMARY KEY,
    cnpj                VARCHAR(20) NOT NULL,
    razao_social        VARCHAR(255) NOT NULL,
    modalidade          VARCHAR(100),
    uf                  VARCHAR(3),
    status              VARCHAR(20) NOT NULL,
    data_carga          TIMESTAMP DEFAULT NOW()
);

-- Índices para operadoras
CREATE INDEX IF NOT EXISTS idx_operadoras_cnpj ON operadoras(cnpj);
CREATE INDEX IF NOT EXISTS idx_operadoras_status ON operadoras(status);
CREATE INDEX IF NOT EXISTS idx_operadoras_uf ON operadoras(uf);
CREATE INDEX IF NOT EXISTS idx_operadoras_reg_ans ON operadoras(reg_ans);

-- Tabelas para despesas SEM dedução
DROP TABLE IF EXISTS despesas_agregadas CASCADE;
CREATE TABLE despesas_agregadas (
    id                          BIGSERIAL PRIMARY KEY,
    cnpj                        VARCHAR(20) NOT NULL,
    razao_social                VARCHAR(255) NOT NULL,
    uf                          VARCHAR(3),
    total_despesas              NUMERIC(18,2) NOT NULL,
    media_despesas_trimestre    NUMERIC(18,2) NOT NULL,
    desvio_padrao_despesas      NUMERIC(18,2) NOT NULL,
    qtd_registros               INTEGER NOT NULL,
    qtd_trimestres              INTEGER NOT NULL,
    qtd_anos                    INTEGER NOT NULL,
    reg_ans                     VARCHAR(20) NOT NULL,
    data_carga                  TIMESTAMP DEFAULT NOW(),
    CONSTRAINT fk_despesas_agregadas_operadoras FOREIGN KEY (reg_ans) REFERENCES operadoras(reg_ans)
);

-- Tabelas para despesas COM dedução
DROP TABLE IF EXISTS despesas_agregadas_c_deducoes CASCADE;
CREATE TABLE despesas_agregadas_c_deducoes (
    id                          BIGSERIAL PRIMARY KEY,
    cnpj                        VARCHAR(20) NOT NULL,
    razao_social                VARCHAR(255) NOT NULL,
    uf                          VARCHAR(3),
    total_despesas              NUMERIC(18,2) NOT NULL,
    media_despesas_trimestre    NUMERIC(18,2) NOT NULL,
    desvio_padrao_despesas      NUMERIC(18,2) NOT NULL,
    qtd_registros               INTEGER NOT NULL,
    qtd_trimestres              INTEGER NOT NULL,
    qtd_anos                    INTEGER NOT NULL,
    reg_ans                     VARCHAR(20) NOT NULL,
    data_carga                  TIMESTAMP DEFAULT NOW(),
    CONSTRAINT fk_despesas_agregadas_c_deducoes_operadoras FOREIGN KEY (reg_ans) REFERENCES operadoras(reg_ans)
);

-- Índices para despesas sem dedução
CREATE INDEX IF NOT EXISTS idx_despesas_agregadas_uf ON despesas_agregadas(uf);
CREATE INDEX IF NOT EXISTS idx_despesas_agregadas_razao_social ON despesas_agregadas(razao_social);
CREATE INDEX IF NOT EXISTS idx_despesas_agregadas_reg_ans ON despesas_agregadas(reg_ans);
CREATE INDEX IF NOT EXISTS idx_despesas_agregadas_cnpj ON despesas_agregadas(cnpj);

-- Índices para despesas com dedução
CREATE INDEX IF NOT EXISTS idx_despesas_agregadas_c_deducoes_uf ON despesas_agregadas_c_deducoes(uf);
CREATE INDEX IF NOT EXISTS idx_despesas_agregadas_c_deducoes_razao_social ON despesas_agregadas_c_deducoes(razao_social);
CREATE INDEX IF NOT EXISTS idx_despesas_agregadas_c_deducoes_reg_ans ON despesas_agregadas_c_deducoes(reg_ans);
CREATE INDEX IF NOT EXISTS idx_despesas_agregadas_c_deducoes_cnpj ON despesas_agregadas_c_deducoes(cnpj);

-- Tabela de consolidados de despesas SEM dedução (detalhamento por trimestre/ano)
DROP TABLE IF EXISTS consolidados_despesas CASCADE;
CREATE TABLE consolidados_despesas (
    id                  BIGSERIAL PRIMARY KEY,
    cnpj                VARCHAR(20) NOT NULL,
    razao_social        VARCHAR(255) NOT NULL,
    trimestre           INTEGER NOT NULL CHECK (trimestre BETWEEN 1 AND 4),
    ano                 INTEGER NOT NULL,
    valor_despesas      NUMERIC(18,2) NOT NULL,
    reg_ans             VARCHAR(20) NOT NULL,
    data_carga          TIMESTAMP DEFAULT NOW(),
    CONSTRAINT fk_consolidados_despesas_operadoras FOREIGN KEY (reg_ans) REFERENCES operadoras(reg_ans)
);

-- Tabela de consolidados de despesas COM dedução (detalhamento por trimestre/ano)
DROP TABLE IF EXISTS consolidados_despesas_c_deducoes CASCADE;
CREATE TABLE consolidados_despesas_c_deducoes (
    id                  BIGSERIAL PRIMARY KEY,
    cnpj                VARCHAR(20) NOT NULL,
    razao_social        VARCHAR(255) NOT NULL,
    trimestre           INTEGER NOT NULL CHECK (trimestre BETWEEN 1 AND 4),
    ano                 INTEGER NOT NULL,
    valor_despesas      NUMERIC(18,2) NOT NULL,
    reg_ans             VARCHAR(20) NOT NULL,
    descricao           VARCHAR(500),
    data_carga          TIMESTAMP DEFAULT NOW(),
    CONSTRAINT fk_consolidados_despesas_c_deducoes_operadoras FOREIGN KEY (reg_ans) REFERENCES operadoras(reg_ans)
);

-- Índices para consolidados_despesas
CREATE INDEX IF NOT EXISTS idx_consolidados_despesas_cnpj ON consolidados_despesas(cnpj);
CREATE INDEX IF NOT EXISTS idx_consolidados_despesas_reg_ans ON consolidados_despesas(reg_ans);
CREATE INDEX IF NOT EXISTS idx_consolidados_despesas_ano_trimestre ON consolidados_despesas(ano, trimestre);
CREATE INDEX IF NOT EXISTS idx_consolidados_despesas_razao_social ON consolidados_despesas(razao_social);

-- Índices para consolidados_despesas_c_deducoes
CREATE INDEX IF NOT EXISTS idx_consolidados_despesas_c_deducoes_cnpj ON consolidados_despesas_c_deducoes(cnpj);
CREATE INDEX IF NOT EXISTS idx_consolidados_despesas_c_deducoes_reg_ans ON consolidados_despesas_c_deducoes(reg_ans);
CREATE INDEX IF NOT EXISTS idx_consolidados_despesas_c_deducoes_ano_trimestre ON consolidados_despesas_c_deducoes(ano, trimestre);
CREATE INDEX IF NOT EXISTS idx_consolidados_despesas_c_deducoes_razao_social ON consolidados_despesas_c_deducoes(razao_social);
