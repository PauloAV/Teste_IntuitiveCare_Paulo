# 🏥 Teste Técnico - Engenharia de Dados (ANS)

Este projeto implementa uma solução completa (End-to-End) para análise de dados financeiros de operadoras de planos de saúde. O pipeline abrange desde a extração automática de dados públicos da ANS (ETL), modelagem em banco de dados SQL e construção de API RESTful com FastAPI.

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

### 4. API e Backend (`src/api/main.py`)
- **Alta Performance:** Construída com **FastAPI** (ASGI) para respostas assíncronas rápidas.
- **Documentação Automática:** Integração nativa com Swagger UI e Redoc.
- **Segurança de Tipos:** Validação de dados de entrada e saída via **Pydantic**.
- **Features:** Paginação real (offset-based), filtros de busca e endpoints analíticos pré-calculados.

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

### 7. Framework da API (Backend) (Item 4.2.1)
**Decisão:** FastAPI (vs Flask).
- **Justificativa:**
    1.  **Documentação:** Gera Swagger UI nativo (requisito implícito para testes de API).
    2.  **Performance:** Suporte nativo a *async/await* para I/O de banco de dados.
    3.  **Segurança de Tipos:** Previne erros de "NullPointer" e tipos incorretos comuns em APIs dinâmicas usando Pydantic.

### 8. Estratégia de Paginação e Filtro (4.2.2)
**Decisão:** Offset-based (`LIMIT` / `OFFSET`) e Server-side Filtering.
- **Justificativa:**
    - **Paginação:** Como os dados são históricos e fechados trimestralmente, não sofremos com "deslocamento de registros" (fantasmas). É a abordagem mais simples para navegação tabular.
    - **Filtro:** Filtrar no servidor (`WHERE ... LIKE`) é mandatório para escalabilidade, evitando trafegar megabytes de JSON para o cliente.

### 9. Otimização de Consultas (Cache vs Pré-cálculo) (4.2.3.)
**Decisão:** Pré-cálculo em Tabela (Data Mart).
- **Justificativa:** Como os dados da ANS têm baixa frequência de atualização (trimestral), o uso de Cache em Memória (Redis) traria complexidade desnecessária. A tabela `despesas_agregadas` criada no ETL funciona como um cache persistente e robusto.

### 10. Estrutura de Resposta da API (Envelope vs Flat List)(4.2.4.)
**Decisão:** Opção B - Resposta Envelopada (`{ data: [...], meta: {...} }`).
- **Justificativa:**
    - **Navegabilidade (UX):** Retornar apenas o array de dados (Opção A) impediria o Frontend de saber quantas páginas existem. O envelope com metadados (`total`, `total_pages`) é essencial para construir componentes de paginação completos (ex: "Página 1 de 50").
    - **Extensibilidade:** O padrão envelopado permite adicionar novos campos de controle no futuro (como mensagens de aviso, status ou links HATEOAS) sem quebrar a estrutura de leitura do cliente, que espera os dados sempre dentro da chave `data`.
    - **Padrão de Indústria:** Segue convenções modernas (como JSON:API), facilitando a integração por desenvolvedores terceiros.

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

### Passo 2: Configuração do Banco de Dados
2. Configure o arquivo `.env` na raiz com suas credenciais do MySQL:
   ```ini
   DB_HOST=localhost
   DB_USER=root
   DB_PASSWORD=sua_senha
   DB_NAME=intuitive_care_test

### Passo 3: Criação do Banco de Dados
3. Carga de Dados (ETL)
Execute o script que cria as tabelas, limpa dados antigos e importa os novos:
    ```Bash
    python src/etl/load.py

### Passo 4: Iniciar a API
4. Iniciar a API
Suba o servidor de aplicação:
    ```Bash
    uvicorn src.api.main:app --reload
    Acesse a documentação interativa em: http://127.0.0.1:8000/docs

# 🚀 Testes e Documentação da API

> ### Endpoints Principais
| Método | Rota | Descrição |
| :--- | :--- | :--- |
| `GET` | `/api/operadoras` | Lista operadoras com paginação e busca por nome. |
| `GET` | `/api/operadoras/{cnpj}` | Detalhes completos (incluindo Endereço via JOIN). |
| `GET` | `/api/operadoras/{cnpj}/despesas` | Histórico financeiro detalhado (Trimestre a Trimestre). |
| `GET` | `/api/estatisticas` | Dashboard: Total Geral, Média e Top 5 Operadoras. |

### 📬 Postman Collection

> [!TIP]
> **Facilite seus testes!** Uma coleção completa foi incluída para validar a API rapidamente.
> 
> 1. Localize o arquivo `Intuitive_Care_API.json` na raiz do projeto.
> 2. No Postman, clique em **Import** e selecione o arquivo.
> 3. Use as requisições pré-configuradas para validar os endpoints automaticamente.