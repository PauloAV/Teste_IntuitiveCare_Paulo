# 🏥 Teste Técnico - Engenharia de Dados (ANS)

Este projeto realiza o pipeline de dados (ETL) para extração, transformação e carregamento de Demonstrações Contábeis das operadoras de planos de saúde, provenientes dos Dados Abertos da ANS.

## 🚀 Funcionalidades

### 1. Extração Automática (`src/etl/extraction.py`)
- **Automação de Datas:** O script calcula automaticamente a data atual e busca os últimos 3 trimestres disponíveis, sem necessidade de alterar o código manualmente.
- **Margem de Segurança:** Implementada lógica de "lag" (atraso) de 120 dias para respeitar o calendário de publicação da ANS e evitar erros 404.
- **Resiliência:** Tratamento de erros de conexão e verificação de integridade dos arquivos ZIP baixados.
- **Organização:** Os arquivos brutos são salvos automaticamente em `data/raw`.

## 🛠️ Decisões Técnicas (Trade-offs)

### Estratégia de Processamento
Optei pelo processamento **em memória (In-Memory)** utilizando Pandas, em vez de processamento em stream/chunks.

**Justificativa:**
Após análise exploratória, verificou-se que o volume total dos arquivos CSV trimestrais (aprox. 150MB descompactados) é significativamente menor que a memória RAM disponível em ambientes de execução padrão (8GB+). A abordagem em memória permite:
1. Maior velocidade de desenvolvimento.
2. Código mais limpo e legível.
3. Aproveitamento das otimizações vetoriais do Pandas.

O processamento incremental (streaming) adicionaria complexidade desnecessária para este volume de dados, sendo uma estratégia reservada caso o dataset escalasse para a ordem de Gigabytes.

## 📦 Como Executar

1. Instale as dependências:
   ```bash
   pip install -r requirements.txt