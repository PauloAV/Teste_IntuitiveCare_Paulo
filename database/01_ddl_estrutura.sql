-- Cria o banco de dados
CREATE DATABASE IF NOT EXISTS intuitive_care_test;
USE intuitive_care_test;

--Tabela de Endereços das Operadoras para evitar redundância
CREATE TABLE IF NOT EXISTS enderecos_operadoras (
    id_endereco BIGINT AUTO_INCREMENT PRIMARY KEY,
    logradouro VARCHAR(255),
    numero VARCHAR(50),
    complemento VARCHAR(255),
    bairro VARCHAR(100),
    cidade VARCHAR(100),
    uf CHAR(2),
    cep VARCHAR(20),
    
    INDEX idx_cidade_uf (cidade, uf)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

--Tabela de Operadoras (Dados Cadastrais)
CREATE TABLE IF NOT EXISTS operadoras (
    registro_ans VARCHAR(20) NOT NULL,
    cnpj VARCHAR(20),
    razao_social VARCHAR(255),
    nome_fantasia VARCHAR(255),
    modalidade VARCHAR(100),
    
    -- Foreign Key para o endereço
    id_endereco BIGINT,
    
    ddd VARCHAR(5),
    telefone VARCHAR(50),
    fax VARCHAR(50),
    endereco_eletronico VARCHAR(255),
    representante VARCHAR(255),
    cargo_representante VARCHAR(100),
    regiao_comercializacao VARCHAR(100),
    data_registro_ans DATE,
    
    PRIMARY KEY (registro_ans),
    FOREIGN KEY (id_endereco) REFERENCES enderecos_operadoras(id_endereco),
    INDEX idx_cnpj (cnpj)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

--Tabela de Despesas Detalhadas
CREATE TABLE IF NOT EXISTS despesas_detalhadas (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    registro_ans VARCHAR(20) NOT NULL,
    cnpj VARCHAR(20),
    razao_social VARCHAR(255),
    trimestre CHAR(2),
    ano INT,
    valor_despesa DECIMAL(15,2),
    cnpj_valido VARCHAR(10),
    modalidade VARCHAR(100),
    uf CHAR(2),
    data_evento DATE,
    
    INDEX idx_registro (registro_ans),
    INDEX idx_ano_trimestre (ano, trimestre)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

--Tabela de Despesas Agregadas (Analytics)
CREATE TABLE IF NOT EXISTS despesas_agregadas (
    razao_social VARCHAR(255),
    uf CHAR(2),
    registro_ans VARCHAR(20),
    modalidade VARCHAR(100),
    total_despesas DECIMAL(15,2),
    media_trimestral DECIMAL(15,2),
    desvio_padrao DECIMAL(15,2),
    
    PRIMARY KEY (registro_ans),
    INDEX idx_total (total_despesas DESC)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;