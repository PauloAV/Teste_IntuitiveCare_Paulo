# 🏥 Teste Técnico - Engenharia de Dados (ANS)

Este projeto realiza o pipeline de dados (ETL) completo: extração, transformação, carregamento (Load) e análise de Demonstrações Contábeis das operadoras de planos de saúde, provenientes dos Dados Abertos da ANS.

## 🚀 Funcionalidades

### 1. Extração Automática (`src/etl/extraction.py`)
- **Automação de Datas:** O script calcula automaticamente a data atual e busca os últimos 3 trimestres disponíveis.
- **Margem de Segurança:** Lógica de "lag" (atraso) de 120 dias para respeitar o calendário de publicação da ANS.
- **Resiliência:** Tratamento de erros de conexão e verificação de integridade (hash/tamanho) dos arquivos ZIP.

### 2. Transformação e Enriquecimento (`src/etl/analysis/complete_analysis.py`)
- **Limpeza:** Padronização de encoding (`Latin1` para `UTF-8`) e tratamento de separadores CSV.
- **Enriquecimento:** Cruzamento de dados financeiros com dados cadastrais (Cadop) via CNPJ.
- **Validação:** Verificação automática de integridade de CNPJ (Módulo 11).

### 3. Banco de Dados e Carga (`src/etl/load.py` & `database/`)
- **Pipeline SQL:** Orquestração automática de scripts `.sql` numerados para garantir a ordem de execução (DDL -> DML -> DQL).
- **Segurança:** Uso de variáveis de ambiente (`.env`) para ocultar credenciais do banco.
- **Performance:** Utilização de `LOAD DATA LOCAL INFILE` para ingestão em massa (bulk load) de arquivos CSV.

---

## 🛠️ Decisões Técnicas (Trade-offs)

### 1. Estratégia de Processamento (Item 1.2)
Optei pelo processamento **em memória (In-Memory)** utilizando Pandas.
- **Justificativa:** O volume total dos arquivos trimestrais (aprox. 150MB) cabe confortavelmente na RAM. O processamento em stream (chunks) adicionaria complexidade desnecessária para este volume.

### 2. Validação de Dados (Item 2.1)
**Desafio:** Como tratar registros financeiros com CNPJs matematicamente inválidos?
- **Estratégia:** *Flagging & Keeping* (Sinalizar e Manter).
- **Justificativa:** Em auditorias contábeis, a integridade da soma financeira é prioritária. Um erro de digitação no cadastro do CNPJ não anula a existência da despesa monetária.

### 3. Enriquecimento e Joins (Item 2.2)
**Desafio:** Garantir que despesas não sejam perdidas se a operadora não estiver no cadastro (Cadop).
- **Estratégia:** `Left Join` utilizando o CNPJ.
- **Justificativa:** Garante a integridade financeira. Campos cadastrais faltantes são preenchidos com "N/D".
- **Deduplicação:** O Cadop sofre `drop_duplicates(keep='last')` antes do join para evitar produto cartesiano (multiplicação de linhas).

### 4. Modelagem de Dados (Item 3.2)
**Trade-off: Normalização vs Desnormalização**
- **Estratégia:** Híbrida.
    - **Tabela `operadoras`:** Normalizada (3FN). Endereços foram separados em uma tabela `enderecos_operadoras` para evitar redundância e anomalias de atualização.
    - **Tabela `despesas_agregadas`:** Desnormalizada.
- **Justificativa:** A normalização facilita a manutenção cadastral, enquanto a tabela agregada desnormalizada otimiza a leitura para dashboards analíticos (BI), evitando JOINs custosos em tempo de leitura.

### 5. Ingestão e Encoding (Item 3.3)
**Desafio:** Arquivos fonte em `Latin1` vs Banco de dados em `UTF-8`.
- **Solução:** Tabelas criadas com charset `utf8mb4`. Comando de importação configurado com `CHARACTER SET latin1`.
- **Justificativa:** Delega ao MySQL a tarefa de conversão de encoding durante a ingestão, garantindo que acentos (ex: "São Paulo") sejam armazenados corretamente sem corromper os dados originais.

### 6. Queries Analíticas (Item 3.4)
**Desafio:** Comparar performance de operadoras com dados faltantes.
- **Estratégia:** Uso de `Common Table Expressions (CTEs)` e agregação prévia (`SUM/GROUP BY`).
- **Justificativa:**
    - **CTEs:** Melhoram a legibilidade e manutenção do código em comparação a subqueries aninhadas.
    - **Agregação:** Necessária para tratar casos onde uma operadora possui múltiplos lançamentos no mesmo trimestre, evitando distorções no ranking de crescimento.

---

## 📊 Arquivos Gerados (`data/processed/`)

1. **`consolidado.csv`**: Base completa de despesas enriquecida.
2. **`despesas_agregadas.csv`**: Relatório analítico final (Totais, Média, Desvio Padrão).
3. **`demonstracoes_contabeis_consolidadas.zip`**: Entrega final compactada.

---

## 📦 Como Executar

### Pré-requisitos
- Python 3.11+
- MySQL Server 8.0+

### Passo 1: Configuração do Ambiente
1. Crie um ambiente virtual e instale as dependências:
   ```bash
   python -m venv .venv
   # Windows:
   .venv\Scripts\activate
   # Linux/Mac:
   source .venv/bin/activate
   
   pip install -r requirements.txt