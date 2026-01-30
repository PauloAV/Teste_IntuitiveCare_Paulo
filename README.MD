# 🏥 Teste Técnico - Engenharia de Dados (ANS)

Este projeto realiza o pipeline de dados (ETL) para extração, transformação e carregamento de Demonstrações Contábeis das operadoras de planos de saúde, provenientes dos Dados Abertos da ANS.

## 🚀 Funcionalidades

### 1. Extração Automática (`src/etl/extraction.py`)
- **Automação de Datas:** O script calcula automaticamente a data atual e busca os últimos 3 trimestres disponíveis, sem necessidade de alterar o código manualmente.
- **Margem de Segurança:** Implementada lógica de "lag" (atraso) de 120 dias para respeitar o calendário de publicação da ANS e evitar erros 404.
- **Resiliência:** Tratamento de erros de conexão e verificação de integridade dos arquivos ZIP baixados.
- **Organização:** Os arquivos brutos são salvos automaticamente em `data/raw`.

## 🛠️ Decisões Técnicas (Trade-offs)

### 1. Estratégia de Processamento
Optei pelo processamento **em memória (In-Memory)** utilizando Pandas.
**Justificativa:** O volume total dos arquivos trimestrais (aprox. 150MB) cabe confortavelmente na RAM (8GB+). O processamento em stream (chunks) adicionaria complexidade desnecessária para este volume, sendo reservado para escalas de Gigabytes.

### 2. Validação de Dados (Item 2.1)
**Desafio:** Como tratar registros financeiros com CNPJs matematicamente inválidos?
- **Estratégia Escolhida:** *Flagging & Keeping* (Sinalizar e Manter).
- **Implementação:** Foi criada uma flag `CNPJ_Valido` usando validação de Módulo 11.
- **Justificativa:** Em auditorias contábeis, a integridade da soma financeira é prioritária. Um erro de digitação no cadastro do CNPJ não anula a existência da despesa monetária.
- **Opção descartada:** Excluir os registros inválidos causaria divergência no balanço total das despesas.

### 3. Enriquecimento e Tratamento de Falhas (Item 2.2)
**Desafio A: Join e Chaves Primárias**
- **Estratégia:** `Left Join` utilizando o CNPJ padronizado (apenas números, 14 dígitos).
- **Justificativa:** O `Left Join` (Financeiro -> Cadastral) garante que nenhuma despesa financeira seja perdida, mesmo que a operadora não seja encontrada na base cadastral (ex: operadoras extintas ou divergências).
- **Registros sem Match:** Campos como `UF`, `Modalidade` e `RegistroANS` foram preenchidos com "N/D" para manter a estrutura tabular.

**Desafio B: Duplicidade no Cadastro (Cadop)**
- **Problema:** O arquivo do Cadop pode conter múltiplos registros históricos para o mesmo CNPJ.
- **Estratégia:** Deduplicação prévia utilizando `drop_duplicates(subset=['CNPJ'], keep='last')`.
- **Justificativa:** Realizar um join com uma base duplicada geraria um **Produto Cartesiano**, multiplicando erroneamente os valores das despesas. Manter o cadastro mais recente garante a unicidade.

### 4. Agregação e Ordenação (Item 2.3)
**Desafio: Performance vs. Legibilidade**
- **Estratégia:** Ordenação descendente (`DESC`) pela coluna `Total_Despesas`.
- **Justificativa:** Baseado no Princípio de Pareto (80/20). Ordenar pelo total permite identificar imediatamente os *big players* e *outliers* do mercado.

## 📊 Arquivos Gerados (`data/processed/`)

1.  **`consolidado.csv`**: Arquivo base contendo todas as despesas trimestrais, enriquecido com dados cadastrais (RegistroANS, Modalidade, UF).
2.  **`demonstracoes_contabeis_consolidadas.zip`**: Versão compactada do consolidado (Entrega do Item 1).
3.  **`despesas_agregadas.csv`**: Relatório analítico final com totais, média trimestral e desvio padrão por operadora (Entrega do Item 2).

---

## 📦 Como Executar

Certifique-se de ter o **Python 3.11.9** instalado (versão utilizada no desenvolvimento).

1. **Crie um ambiente virtual (opcional, mas recomendado):**
   ```bash
   python -m venv .venv
   source .venv/bin/activate  # Linux/Mac
   .venv\Scripts\activate     # Windows
   
2. **Instale as dependências:**
   ```bash
   pip install -r requirements.txt