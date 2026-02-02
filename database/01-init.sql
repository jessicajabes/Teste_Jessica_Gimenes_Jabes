SET client_encoding = 'UTF8';

DO $$
BEGIN
  IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = 'jessica') THEN
    CREATE ROLE jessica LOGIN PASSWORD '1234';
  END IF;
END
$$;

-- criar DB somente se não existir (bloco PL/pgSQL seguro)
DO $$
BEGIN
  IF NOT EXISTS (SELECT FROM pg_database WHERE datname = 'intuitive_care') THEN
    EXECUTE 'CREATE DATABASE intuitive_care OWNER jessica';
  END IF;
END
$$;

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

CREATE TABLE IF NOT EXISTS demonstracoes_contabeis_temp (
  id SERIAL PRIMARY KEY,
  data DATE,
  reg_ans VARCHAR(8) NOT NULL,
  cd_conta_contabil VARCHAR(9) NOT NULL,
  descricao VARCHAR(150),
  vl_saldo_inicial DECIMAL(15, 2),
  vl_saldo_final DECIMAL(15, 2),
  valor_trimestre DECIMAL(15, 2),
  trimestre INTEGER NOT NULL,
  ano INTEGER NOT NULL,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  CONSTRAINT uk_demonstracao UNIQUE (reg_ans, cd_conta_contabil, trimestre, ano)
);

CREATE INDEX IF NOT EXISTS idx_demonstracoes_data ON demonstracoes_contabeis_temp(data);
CREATE INDEX IF NOT EXISTS idx_demonstracoes_reg_ans ON demonstracoes_contabeis_temp(reg_ans);
CREATE INDEX IF NOT EXISTS idx_demonstracoes_conta ON demonstracoes_contabeis_temp(cd_conta_contabil);
CREATE INDEX IF NOT EXISTS idx_demonstracoes_trimestre_ano ON demonstracoes_contabeis_temp(trimestre, ano);

-- Tabela de operadoras (ativas e canceladas) - permite duplicatas
CREATE TABLE IF NOT EXISTS operadoras (
  id SERIAL PRIMARY KEY,
  reg_ans VARCHAR(8) NOT NULL,
  cnpj VARCHAR(20),
  razao_social VARCHAR(255),
  modalidade VARCHAR(100),
  uf VARCHAR(2),
  status VARCHAR(20), -- 'ATIVA' ou 'CANCELADA'
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_operadoras_registro ON operadoras(reg_ans);
CREATE INDEX IF NOT EXISTS idx_operadoras_status ON operadoras(status);