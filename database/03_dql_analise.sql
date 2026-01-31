USE intuitive_care_test;


-- QUERY 1: Top 5 operadoras com maior crescimento percentual
-- PERGUNTA: Quais as 5 operadoras com maior crescimento das despesas?
-- DESAFIO: Como tratar dados faltantes?
-- DECISÃO: Utilizei INNER JOIN para considerar apenas operadoras ativas no início
--          e no fim do período. Operadoras sem dados no 1T não possuem base para
--          cálculo de crescimento (divisão por zero), e operadoras sem dados no
--          3T não representam crescimento, mas sim saída do mercado.

WITH primeiro_trimestre AS (
    SELECT registro_ans, SUM(valor_despesa) as valor_inicial
    FROM despesas_detalhadas
    WHERE ano = 2025 AND trimestre = '1T'
    GROUP BY registro_ans
),
ultimo_trimestre AS (
    SELECT registro_ans, SUM(valor_despesa) as valor_final
    FROM despesas_detalhadas
    WHERE ano = 2025 AND trimestre = '3T'
    GROUP BY registro_ans
)
SELECT 
    o.razao_social,
    pt.valor_inicial,
    ut.valor_final,
    ROUND(((ut.valor_final - pt.valor_inicial) / pt.valor_inicial) * 100, 2) AS crescimento_percentual
FROM operadoras o
JOIN primeiro_trimestre pt ON o.registro_ans = pt.registro_ans
JOIN ultimo_trimestre ut ON o.registro_ans = ut.registro_ans
WHERE pt.valor_inicial > 0
ORDER BY crescimento_percentual DESC
LIMIT 5;


-- QUERY 2: Distribuição por UF
-- PERGUNTA: Top 5 estados com maiores despesas totais.
-- DESAFIO: Calcular também a média por operadora.
-- SOLUÇÃO: Utilização das funções de agregação SUM() para o total absoluto e
--          AVG() para a média por linha (operadora) dentro do agrupamento por UF.

SELECT 
    uf,
    SUM(total_despesas) AS total_despesas_estado,
    AVG(total_despesas) AS media_por_operadora,
    COUNT(DISTINCT registro_ans) AS qtd_operadoras
FROM despesas_agregadas
WHERE uf <> 'ND'
GROUP BY uf
ORDER BY total_despesas_estado DESC
LIMIT 5;

-- QUERY 3: Operadoras acima da média em 2+ trimestres
-- PERGUNTA: Quantas operadoras performaram acima da média? (Listagem abaixo)
-- TRADE-OFF TÉCNICO: CTEs vs Subqueries vs Window Functions.
-- ESCOLHA: Common Table Expressions (CTEs).
-- JUSTIFICATIVA: 
-- 1. Legibilidade: Separa a lógica de cálculo da média (benchmark) da lógica
--    de comparação, facilitando o entendimento por outros desenvolvedores.
-- 2. Manutenibilidade: Facilita ajustes futuros na regra da média sem quebrar
--    a query principal.
-- 3. Performance: MySQL 8.0 otimiza bem CTEs não-recursivas, sendo superior a
--    subqueries correlacionadas em grandes volumes de dados.

WITH despesas_consolidadas_trimestre AS (
    -- Agrega despesas por operadora e trimestre
    SELECT registro_ans, trimestre, SUM(valor_despesa) as despesa_total_trimestre
    FROM despesas_detalhadas
    GROUP BY registro_ans, trimestre
),
media_mercado_por_trimestre AS (
    -- Calcula a média de despesas por trimestre
    SELECT trimestre, AVG(despesa_total_trimestre) as media_mercado
    FROM despesas_consolidadas_trimestre
    GROUP BY trimestre
),
performance_operadora AS (
    -- Compara despesas da operadora com a média do mercado
    SELECT 
        d.registro_ans,
        d.trimestre,
        CASE WHEN d.despesa_total_trimestre > m.media_mercado THEN 1 ELSE 0 END as acima_da_media
    FROM despesas_consolidadas_trimestre d
    JOIN media_mercado_por_trimestre m ON d.trimestre = m.trimestre
)
SELECT 
    o.registro_ans,
    o.razao_social,
    SUM(p.acima_da_media) as qtd_trimestres_acima
FROM performance_operadora p
JOIN operadoras o ON p.registro_ans = o.registro_ans
GROUP BY o.registro_ans, o.razao_social
HAVING qtd_trimestres_acima >= 2
ORDER BY qtd_trimestres_acima DESC, o.razao_social
LIMIT 10; -- Limita visualização para não poluir o terminal