USE intuitive_care_test;
SET GLOBAL local_infile = 1;

SET FOREIGN_KEY_CHECKS = 0; -- Desativa verificação de chaves para poder limpar
TRUNCATE TABLE despesas_detalhadas;
TRUNCATE TABLE despesas_agregadas;
SET FOREIGN_KEY_CHECKS = 1; -- Reativa verificação de chaves
-- PREPARAÇÃO: Tabela Temporária

DROP TEMPORARY TABLE IF EXISTS temp_cadop;
CREATE TEMPORARY TABLE temp_cadop (
    registro_ans VARCHAR(20),
    cnpj VARCHAR(20),
    razao_social VARCHAR(255),
    nome_fantasia VARCHAR(255),
    modalidade VARCHAR(100),
    logradouro VARCHAR(255),
    numero VARCHAR(50),
    complemento VARCHAR(255),
    bairro VARCHAR(100),
    cidade VARCHAR(100),
    uf CHAR(2),
    cep VARCHAR(20),
    ddd VARCHAR(5),
    telefone VARCHAR(50),
    fax VARCHAR(50),
    endereco_eletronico VARCHAR(255),
    representante VARCHAR(255),
    cargo_representante VARCHAR(100),
    regiao_comercializacao VARCHAR(100),
    data_registro_ans VARCHAR(20)
) DEFAULT CHARSET=utf8mb4; 


-- IMPORTAÇÃO DO CADOP (Correções Aplicadas)


-- MELHORIA: path dinâmico — '__DATA_RAW__' é substituído pelo load.py com o caminho absoluto real do projeto
LOAD DATA LOCAL INFILE '__DATA_RAW__/Relatorio_Cadop.csv'
INTO TABLE temp_cadop
CHARACTER SET utf8mb4              
FIELDS TERMINATED BY ';'
OPTIONALLY ENCLOSED BY '"'         
LINES TERMINATED BY '\n'           
IGNORE 1 ROWS;


--NORMALIZAÇÃO: Endereços

INSERT IGNORE INTO enderecos_operadoras (logradouro, numero, complemento, bairro, cidade, uf, cep)
SELECT DISTINCT logradouro, numero, complemento, bairro, cidade, uf, cep
FROM temp_cadop;


--NORMALIZAÇÃO: Operadoras

INSERT IGNORE INTO operadoras (
    registro_ans, cnpj, razao_social, nome_fantasia, modalidade, 
    id_endereco,
    ddd, telefone, fax, endereco_eletronico, representante, cargo_representante, 
    regiao_comercializacao, data_registro_ans
)
SELECT 
    t.registro_ans, t.cnpj, t.razao_social, t.nome_fantasia, t.modalidade,
    e.id_endereco,
    t.ddd, t.telefone, t.fax, t.endereco_eletronico, t.representante, t.cargo_representante, 
    t.regiao_comercializacao,
    -- Tratamento robusto para Data
    CASE 
        WHEN t.data_registro_ans = '' OR t.data_registro_ans IS NULL THEN NULL 
        ELSE STR_TO_DATE(TRIM(BOTH '\r' FROM t.data_registro_ans), '%Y-%m-%d') 
    END
FROM temp_cadop t
-- LEFT JOIN garante que a Operadora entra mesmo se o endereço falhar no match
LEFT JOIN enderecos_operadoras e ON 
    (t.logradouro <=> e.logradouro) AND
    (t.numero <=> e.numero) AND
    (t.cep <=> e.cep);

DROP TEMPORARY TABLE temp_cadop;


-- IMPORTAÇÃO: Despesas (Consolidado)

<<<<<<< HEAD
-- MELHORIA: path dinâmico — '__DATA_PROCESSED__' é substituído pelo load.py com o caminho absoluto real do projeto
LOAD DATA LOCAL INFILE '__DATA_PROCESSED__/consolidado.csv'
=======
LOAD DATA LOCAL INFILE '{{CAMINHO_PROCESSED}}/consolidado.csv'
>>>>>>> e38b1063f839f7f458b092882b80d00957df451e
INTO TABLE despesas_detalhadas
CHARACTER SET utf8mb4             
FIELDS TERMINATED BY ';'
LINES TERMINATED BY '\r\n'         
IGNORE 1 ROWS
(cnpj, razao_social, trimestre, @v_ano, @v_valor, cnpj_valido, registro_ans, modalidade, uf)
SET 
    ano = CAST(@v_ano AS UNSIGNED),
    valor_despesa = CAST(REPLACE(@v_valor, ',', '.') AS DECIMAL(15,2)),
    data_evento = STR_TO_DATE(CONCAT(@v_ano, '-', CASE trimestre WHEN '1T' THEN '01' WHEN '2T' THEN '04' WHEN '3T' THEN '07' ELSE '10' END, '-01'), '%Y-%m-%d');


-- IMPORTAÇÃO: Agregadas
<<<<<<< HEAD
LOAD DATA LOCAL INFILE '__DATA_PROCESSED__/despesas_agregadas.csv'
=======
LOAD DATA LOCAL INFILE '{{CAMINHO_PROCESSED}}/despesas_agregadas.csv'
>>>>>>> e38b1063f839f7f458b092882b80d00957df451e
INTO TABLE despesas_agregadas
CHARACTER SET utf8mb4
FIELDS TERMINATED BY ';'
LINES TERMINATED BY '\r\n'
IGNORE 1 ROWS
(razao_social, uf, registro_ans, modalidade, @v_total, @v_media, @v_desvio)
SET
    total_despesas = CAST(REPLACE(@v_total, ',', '.') AS DECIMAL(15,2)),
    media_trimestral = CAST(REPLACE(@v_media, ',', '.') AS DECIMAL(15,2)),
    desvio_padrao = CAST(REPLACE(@v_desvio, ',', '.') AS DECIMAL(15,2));