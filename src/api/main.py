from fastapi import FastAPI, HTTPException, Query, Path
from src.api.database_utils import get_database_connection
from typing import List, Optional
from pydantic import BaseModel


app = FastAPI(
    title = 'API Intuitive Care',
    description='API de dados da ANS com paginação e Analytics',
    version='1.0.0'
)
# Definição dos modelos de dados
class Endereco(BaseModel):
    logradouro: Optional[str] = None
    numero: Optional[str] = None
    cidade: Optional[str] = None
    uf: Optional[str] = None


class OperadoraSimples(BaseModel):
    registro_ans: str
    cnpj: str
    razao_social: str
    modalidade: str

class OperadoraDetalhada(OperadoraSimples):
    nome_fantasia: Optional[str] = None
    rpresentante: Optional[str] = None
    telefone: Optional[str] = None
    endereco: Optional[Endereco] = None

class DespesaHistorico(BaseModel):
    trimestre: str
    ano: int
    valor_despesa: float
    data_evento: str

class TopOperadora(BaseModel):
    razao_social: str
    total_despesas: float

class EstatisticasGerais(BaseModel):
    total_despesas_geral: float
    media_despesas_trimestral: float
    top_5_maiores_despesas:List[TopOperadora]

class MetaData(BaseModel):
    total: int
    page: int
    limit: int
    total_pages: int


class PaginatedResponse(BaseModel):
    data: List[OperadoraSimples]
    meta: MetaData


# Rotas da API

@app.get('/')

def read_root():
    return {'message': 'API Online! Rotas disponíveis em /api/...'}

@app.get('/api/operadoras', response_model=PaginatedResponse)

def listar_operadoras(
    page: int = Query(1, ge=1),
    limit: int = Query(10, ge=1, le=2000),
    termo: str = Query('', description='Filtro por Razão Social')
):
    conn = get_database_connection()
    cursor = conn.cursor(dictionary=True)
    offset = (page - 1) * limit

    # contagem
    cursor.execute(' SELECT COUNT(*) AS total FROM operadoras WHERE razao_social LIKE %s', (f'%{termo}%',))
    total = cursor.fetchone()['total']

    #busca
    query = '''
        SELECT registro_ans, cnpj, razao_social, modalidade
        FROM operadoras
        WHERE razao_social LIKE %s
        ORDER BY razao_social
        LIMIT %s OFFSET %s
    '''
    cursor.execute(query, (f'%{termo}%', limit, offset))
    resultados = cursor.fetchall()
    cursor.close()
    conn.close()

    return{
        'data': resultados,
        'meta': {
            'total': total,
            'page': page,
            'limit': limit,
            'total_pages': (total + limit - 1) // limit
        }
    }

# detalhes da operadora
@app.get('/api/operadoras/{cnpj}', response_model=OperadoraDetalhada)

def detalhes_operadora(cnpj: str = Path(..., description='CNPJ apenas números')):
    conn = get_database_connection()
    cursor = conn.cursor(dictionary=True)

    query = '''SELECT 
            o.registro_ans, o.cnpj, o.razao_social, o.nome_fantasia, o.modalidade,
            o.representante, o.telefone,
            e.logradouro, e.numero, e.cidade, e.uf
        FROM operadoras o
        LEFT JOIN enderecos_operadoras e ON o.id_endereco = e.id_endereco
        WHERE o.cnpj = %s
    '''

    cursor.execute(query, (cnpj,))
    resultado = cursor.fetchone()
    cursor.close()
    conn.close()

    if not resultado:
        raise HTTPException(status_code=404, detail='Operadora não encontrada')
    
    return{
        "registro_ans": resultado['registro_ans'],
        "cnpj": resultado['cnpj'],
        "razao_social": resultado['razao_social'],
        "modalidade": resultado['modalidade'],
        "nome_fantasia": resultado['nome_fantasia'],
        "representante": resultado['representante'],
        "telefone": resultado['telefone'],
        "endereco": {
            "logradouro": resultado['logradouro'],
            "numero": resultado['numero'],
            "cidade": resultado['cidade'],
            "uf": resultado['uf']
        }
    }

# --- Rota 3: Histórico de Despesas ---
@app.get('/api/operadoras/{cnpj}/despesas', response_model=List[DespesaHistorico])
def historico_despesas(cnpj: str):
    conn = get_database_connection()
    cursor = conn.cursor(dictionary=True)

    query = """
        SELECT trimestre, ano, valor_despesa, DATE_FORMAT(data_evento, '%Y-%m-%d') as data_evento
        FROM despesas_detalhadas
        WHERE cnpj = %s
        ORDER BY ano DESC, trimestre DESC
    """
    cursor.execute(query, (cnpj,))
    resultados = cursor.fetchall()
    
    cursor.close()
    conn.close()

    if not resultados:
        # Se a operadora existe mas não tem despesas, retorna lista vazia (código 200)
        # Se quiser erro 404 caso a operadora não exista, precisaria validar antes.
        return []

    return resultados
# Histórico de despesas

@app.get('/api/estatisticas', response_model=EstatisticasGerais)
def estatisticas_gerais():
    conn = get_database_connection()
    cursor = conn.cursor(dictionary=True)

    # Total de despesas geral
    cursor.execute(''' SELECT 
                   SUM(total_despesas) AS total_despesas_geral, 
                   AVG(total_despesas) AS media_despesas_trimestral 
                   FROM despesas_agregadas
                   ''')
    geral = cursor.fetchone()

    # Top 5 maiores despesas
    cursor.execute('''
        SELECT razao_social, total_despesas
        FROM despesas_agregadas
        ORDER BY total_despesas DESC
        LIMIT 5
    ''')
    top5 = cursor.fetchall()

    cursor.close()
    conn.close()

    return {
        "total_despesas_geral": geral['total_despesas_geral'] or 0.0,
        "media_despesas_trimestral": geral['media_despesas_trimestral'] or 0.0,
        "top_5_maiores_despesas": top5
    }