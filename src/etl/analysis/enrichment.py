import pandas as pd
import numpy as np
import os
import re

# Caminhos dos arquivos
PASTA_PROCESSED = os.path.join('data', 'processed')
PASTA_RAW = os.path.join('data', 'raw')
ARQUIVO_CONSOLIDADO = os.path.join(PASTA_PROCESSED, 'consolidado.csv')
ARQUIVO_CADOP = os.path.join(PASTA_RAW, 'Relatorio_Cadop.csv')
ARQUIVO_AGREGADO = os.path.join(PASTA_PROCESSED, 'despesas_agregadas.csv')

def limpar_cnpj(valor):
    '''
    Padroniza CNPJ: remove pontuacao e garante 14 digitos.
    '''
    if pd.isna(valor): return ''
    limpo = re.sub(r'[^0-9]', '', str(valor))
    return limpo.zfill(14)

def validar_cnpj_matematicamente(cnpj):
    '''
    Item 2.1: Validacao matematica do CNPJ.
    '''
    cnpj = limpar_cnpj(cnpj)
    
    if len(cnpj) != 14 or len(set(cnpj)) == 1:
        return False

    pesos1 = [5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2]
    soma1 = sum(int(a) * b for a, b in zip(cnpj[:12], pesos1))
    resto1 = soma1 % 11
    digito1 = 0 if resto1 < 2 else 11 - resto1

    if int(cnpj[12]) != digito1: return False

    pesos2 = [6, 5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2]
    soma2 = sum(int(a) * b for a, b in zip(cnpj[:13], pesos2))
    resto2 = soma2 % 11
    digito2 = 0 if resto2 < 2 else 11 - resto2

    return int(cnpj[13]) == digito2

def executar_pipeline_completo():
    print('[INFO] Iniciando Pipeline de Enriquecimento e Agregacao...')

    # CARREGA DADOS FINANCEIROS
    if not os.path.exists(ARQUIVO_CONSOLIDADO):
        print('[ERRO] Arquivo consolidado.csv nao encontrado.')
        return

    print('[INFO] Lendo consolidado.csv...')
    
    df = pd.read_csv(ARQUIVO_CONSOLIDADO, sep=';', encoding='utf-8-sig', dtype=str)
    
    # Pre-processamento
    df['ValorDespesas'] = pd.to_numeric(df['ValorDespesas'].str.replace(',', '.'), errors='coerce').fillna(0)
    df['KEY_CNPJ'] = df['CNPJ'].apply(limpar_cnpj)

    # 2. VALIDACAO 
    print('[INFO] Aplicando validacoes...')
    # Cria a coluna de validacao no proprio consolidado
    df['CNPJ_Valido'] = df['KEY_CNPJ'].apply(validar_cnpj_matematicamente)
    
    # Filtro de consistencia basica (Razao Social existente e valor > 0)
    df = df[df['RazaoSocial'].notna() & (df['RazaoSocial'] != '')]
    # Nota: Nao remove CNPJs invalidos matematicamente (Trade-off: Flagging), 
    # mas mantemos apenas quem tem despesa para a analise
    
    # 3. ENRIQUECIMENTO
    print('[INFO] Cruzando com Cadop para adicionar RegistroANS, Modalidade e UF...')
    
    if os.path.exists(ARQUIVO_CADOP):
        df_cadop = pd.read_csv(
            ARQUIVO_CADOP, sep=';', encoding='latin1', dtype=str,
            usecols=['CNPJ', 'REGISTRO_OPERADORA', 'Modalidade', 'UF']
        )
        
        # Limpeza e Deduplicacao do Cadop
        df_cadop['KEY_CNPJ'] = df_cadop['CNPJ'].apply(limpar_cnpj)
        df_cadop = df_cadop.drop_duplicates(subset=['KEY_CNPJ'], keep='last')
        
        # JOIN (Left Join para enriquecer o consolidado)
        df_enriquecido = pd.merge(
            df, 
            df_cadop[['KEY_CNPJ', 'REGISTRO_OPERADORA', 'Modalidade', 'UF']], 
            on='KEY_CNPJ', 
            how='left'
        )
        
        # Renomeia coluna REGISTRO_OPERADORA para RegistroANS conforme pedido
        df_enriquecido = df_enriquecido.rename(columns={'REGISTRO_OPERADORA': 'RegistroANS'})
        
        # Preenche falhas de cruzamento
        df_enriquecido['RegistroANS'] = df_enriquecido['RegistroANS'].fillna('N/D')
        df_enriquecido['Modalidade'] = df_enriquecido['Modalidade'].fillna('Desconhecida')
        df_enriquecido['UF'] = df_enriquecido['UF'].fillna('N/D')
        
        # Remove a chave auxiliar usada apenas para o join
        df_enriquecido.drop(columns=['KEY_CNPJ'], inplace=True)
        
    else:
        print('[AVISO] Cadop nao encontrado. Preenchendo com N/D.')
        df_enriquecido = df
        df_enriquecido['RegistroANS'] = 'N/D'
        df_enriquecido['Modalidade'] = 'N/D'
        df_enriquecido['UF'] = 'N/D'
        if 'KEY_CNPJ' in df_enriquecido.columns:
             df_enriquecido.drop(columns=['KEY_CNPJ'], inplace=True)

    # 4. SALVAR O CONSOLIDADO ENRIQUECIDO 
    print(f'[INFO] Sobrescrevendo {ARQUIVO_CONSOLIDADO} com colunas adicionadas...')
    # Salvamos em latin1 conforme solicitado
    df_enriquecido.to_csv(ARQUIVO_CONSOLIDADO, index=False, sep=';', encoding='latin1', float_format='%.2f', errors='replace')

    # 5. AGREGACAO E ESTATISTICA
    # Gera o arquivo despesas_agregadas.csv a partir do consolidado ja enriquecido
    print('[INFO] Gerando despesas_agregadas.csv...')
    
    # Filtra apenas despesas positivas para a estatistica
    df_stats = df_enriquecido[df_enriquecido['ValorDespesas'] > 0].copy()
    
    # Agrupamento solicitado
    cols_group = ['RazaoSocial', 'UF', 'RegistroANS', 'Modalidade']
    
    df_agg = df_stats.groupby(cols_group)['ValorDespesas'].agg(
        Total_Despesas='sum',
        Media_Trimestral='mean',
        Desvio_Padrao='std'
    ).reset_index()
    
    # Tratamento final
    df_agg['Desvio_Padrao'] = df_agg['Desvio_Padrao'].fillna(0)
    df_agg = df_agg.sort_values(by='Total_Despesas', ascending=False)

    # SALVA ARQUIVO AGREGADO
    print(f'[INFO] Salvando {ARQUIVO_AGREGADO}...')
    df_agg.to_csv(ARQUIVO_AGREGADO, index=False, sep=';', encoding='latin1', float_format='%.2f', errors='replace')
    
    print('------------------------------')
    print('[SUCESSO] Processo concluido.')
    print('1. consolidado.csv atualizado com RegistroANS, Modalidade e UF.')
    print('2. despesas_agregadas.csv gerado com as estatisticas.')
    print(df_agg.head())

if __name__ == '__main__':
    executar_pipeline_completo()