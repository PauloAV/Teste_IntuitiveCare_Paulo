import os
import requests
import zipfile
from datetime import datetime, timedelta

 # Definições de constantes
BASE_URL = 'https://dadosabertos.ans.gov.br/FTP/PDA/demonstracoes_contabeis/'
PASTA_RAW = os.path.join('data', 'raw')

def obter_trimestres_recentes() -> list[str]:
    trimestres = []

    data_ref = datetime.now() - timedelta(days=180)

    while len(trimestres) < 3:
        ano = data_ref.year
        trimestre_num = (data_ref.month - 1) // 3 + 1
        periodo = f'{ano}/{trimestre_num}T'

        if trimestre_num not in trimestres:
            trimestres.append(periodo)

        data_ref -= timedelta(days=90)

    return sorted(trimestres)

def baixar_arquivos_ans(trimestres: list[str]) -> None:
    #Baixa e extrai os arquivos ZIP dos trimestres fornecidos
    if not os.path.exists(PASTA_RAW):
        os.makedirs(PASTA_RAW)
        print(f'Pasta criada: {PASTA_RAW}')

    print(f'Buscando os seguintes períodos: {trimestres}')

    for trimestre in trimestres:

        ano = trimestre.split('/')[0]
        periodo = trimestre.split('/')[1]


        nome_arquivo = f'{periodo}{ano}.zip'
        url = f'{BASE_URL}{ano}/{nome_arquivo}'

        nome_arquivo_local = f'{ano}_{periodo}_demonstracoes_contabeis.zip'
        caminho_zip = os.path.join(PASTA_RAW, nome_arquivo_local)

        print(f'Baixando {trimestre}...')

        try:
            response = requests.get(url, stream=True, timeout=60)

            if response.status_code == 200:
                with open(caminho_zip, 'wb') as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        f.write(chunk)

                print(f'Arquivo baixado: {caminho_zip}')
                try:
                    with zipfile.ZipFile(caminho_zip, 'r') as zip_ref:
                        zip_ref.extractall(PASTA_RAW)
                    print(f'Arquivo extraído: {caminho_zip}')
                except zipfile.BadZipFile:
                    print(f'Erro: Arquivo corrompido de {caminho_zip}')

            else:
                print(f'Não encontrado(Erro {response.status_code}): {url}')
                print('Dica: a ANS pode não ter disponibilizado o arquivo ainda.')
        except Exception as e:
            print(f'Erro crítico em {url}: {e}')
    print("\n Processo finalizado.")
if __name__ == '__main__':

    lista_trimestres = obter_trimestres_recentes()
    baixar_arquivos_ans(lista_trimestres)