import pandas as pd
import os
import glob
import zipfile


PASTA_RAW = os.path.join('data', 'raw')
PASTA_PROCESSED = os.path.join('data', 'processed')
ARQUIVO_ZIP_FINAL = os.path.join(PASTA_PROCESSED, 'demonstracoes_contabeis_consolidadas.zip')
CAMINHO_CADOP = os.path.join(PASTA_RAW, 'Relatorio_Cadop.csv')


def carregar_cadop() -> None:
    if not os.path.exists(CAMINHO_CADOP):
        print('Aviso: Arquivo Relatorio_Cadop.csv não encontrado. Usaremos IDs.')
        return {}, {}
    
    try:
        df_cadop = pd.read_csv(
            CAMINHO_CADOP, 
            sep=';', 
            encoding='latin1',
            usecols=['REGISTRO_OPERADORA', 'CNPJ', 'Razao_Social'],
            dtype = {'REGISTRO_OPERADORA': str, 'CNPJ': str}
        )

        mapa_nomes = df_cadop.set_index('REGISTRO_OPERADORA')['Razao_Social'].to_dict()


        mapa_cnpjs = df_cadop.set_index('REGISTRO_OPERADORA')['CNPJ'].to_dict()

        return mapa_nomes, mapa_cnpjs
    
    except Exception as e:
        print(f'Erro ao carregar o arquivo CADOP: {e}')
        return {}, {}
    
def transformar_dados() -> None:
    if not  os.path.exists(PASTA_PROCESSED):
        os.makedirs(PASTA_PROCESSED)

    mapa_nomes, mapa_cnpjs = carregar_cadop()

    todos_csvs = glob.glob(os.path.join(PASTA_RAW, '**', '*.csv'), recursive=True)
    arquivos_contabeis = [
        f for f in todos_csvs 
        if 'Relatorio_cadop' not in f and 'cadop' not in f.lower()
    ]

    if not arquivos_contabeis:
        print('Nenhum arquivo contábil encontrado para transformação.')
        return
    
    print(f'Processando {len(arquivos_contabeis)} arquivos contábeis...')
    lista_dfs = []

    for arquivo in arquivos_contabeis:
        try:
            df = pd.read_csv(
                arquivo,
                sep=';',
                encoding='latin1',
                thousands='.',
                dtype={'REG_ANS': str}

            )

            df['CD_CONTA_CONTABIL'] = df['CD_CONTA_CONTABIL'].astype(str)
            df = df[df['CD_CONTA_CONTABIL'].str.startswith('4')]

            df['RazaoSocial'] = df['REG_ANS'].map(mapa_nomes)
            df['CNPJ'] = df['REG_ANS'].map(mapa_cnpjs)

            # Se não achou o nome (NaN), preenche com "Operadora + ID"
            df['RazaoSocial'] = df['RazaoSocial'].fillna('Operadora ' + df['REG_ANS'])
            # Se não achou o CNPJ (NaN), usa o próprio ID da ANS provisoriamente
            df['CNPJ'] = df['CNPJ'].fillna(df['REG_ANS'])

            # Renomeia apenas a coluna de Valor
            df = df.rename(columns={'VL_SALDO_FINAL': 'ValorDespesas'})
            # Extrai Ano e Trimestre do nome do arquivo
            if 'DATA' in df.columns:
                # Converte para data
                df['DATA'] = pd.to_datetime(df['DATA'], errors='coerce')
                
                # Extrai o Ano
                df['Ano'] = df['DATA'].dt.year
                
                # Calcula o Trimestre baseado no mês (Mês 1-3=1T, 4-6=2T, etc.)
                df['Trimestre'] = df['DATA'].dt.month.apply(lambda x: f"{(int(x)-1)//3 + 1}T" if pd.notnull(x) else "N/D")
            else:
                # Se não tiver coluna DATA, tenta fallback pelo nome (mas DATA é o padrão)
                print(f"Aviso: Arquivo {os.path.basename(arquivo)} sem coluna DATA.")
                df['Ano'] = 2025
                df['Trimestre'] = 'N/D'

            # Preenche Ano nulo com 2025 (segurança)
            df['Ano'] = df['Ano'].fillna(2025).astype(int)

            # Seleciona colunas
            colunas = ['CNPJ', 'RazaoSocial', 'Trimestre', 'Ano', 'ValorDespesas']
            for col in colunas:
                if col not in df.columns: df[col] = 0

            lista_dfs.append(df[colunas])

        except Exception as e:
            print(f'Erro em {os.path.basename(arquivo)}: {e}')
    # Concatenar e salvar o CSV final
    if lista_dfs:
        print('Consolidando dados...')
        
        # Concatena todos os DataFrames em um único DataFrame
        df_final = pd.concat(lista_dfs, ignore_index=True)

        # Tratamento numérico
        df_final['ValorDespesas'] = pd.to_numeric(df_final['ValorDespesas'], errors='coerce').fillna(0)
        df_final = df_final[df_final['ValorDespesas'] != 0]
        df_final['ValorDespesas'] = df_final['ValorDespesas'].abs()

        # AGORA SIM fazemos o GroupBy no DataFrame final
        print('📊 Agrupando valores...')
        df_final = df_final.groupby(['CNPJ', 'RazaoSocial', 'Trimestre', 'Ano'])['ValorDespesas'].sum().reset_index()

        # Salva CSV temporário
        csv_path = os.path.join(PASTA_PROCESSED, 'consolidado.csv')
        df_final.to_csv(csv_path, index=False, sep=';', encoding='utf-8', float_format='%.2f')

        # Cria ZIP
        print(f'Compactando para {ARQUIVO_ZIP_FINAL}...')
        with zipfile.ZipFile(ARQUIVO_ZIP_FINAL, 'w', zipfile.ZIP_DEFLATED) as zf:
            zf.write(csv_path, arcname='consolidado_despesas.csv')
        
        print(f'Sucesso! Arquivo gerado em: {ARQUIVO_ZIP_FINAL}')
        

if __name__ == '__main__':
    transformar_dados()