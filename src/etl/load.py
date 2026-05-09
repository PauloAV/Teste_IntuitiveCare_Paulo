import mysql.connector
import os
from dotenv import load_dotenv

# CONFIGURAÇÕES DO BANCO DE DADOS 
load_dotenv()
DB_HOST = os.getenv("DB_HOST")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD") 
DB_NAME = os.getenv("DB_NAME")

# Caminhos dos arquivos
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(os.path.dirname(BASE_DIR))
SQL_DIR = os.path.join(PROJECT_ROOT, "database")

# Definição das pastas de dados
RAW_DIR = os.path.join(PROJECT_ROOT, "data", "raw")
PROCESSED_DIR = os.path.join(PROJECT_ROOT, "data", "processed")
RAW_DIR_MYSQL = RAW_DIR.replace('\\', '/')
PROCESSED_DIR_MYSQL = PROCESSED_DIR.replace('\\', '/')

# Arquivos na ordem exata de execução
SQL_FILES = [
    "01_ddl_estrutura.sql",
    "02_dml_importacao.sql",
    "03_dql_analise.sql"
]

def get_db_connection():
   #Cria a conexão com o MySQL permitindo carga de arquivos locais.
    return mysql.connector.connect(
        host=DB_HOST,
        user=DB_USER,
        password=DB_PASSWORD,
        database=DB_NAME,
    commands = []
    current_char_list = []
    in_quote = None  # Pode ser ' ou "
    escape = False

    for char in text:
        # Se o caractere anterior foi uma barra invertida, este é literal
        if escape:
            current_char_list.append(char)
            escape = False
            continue

        if char == '\\':
            escape = True
            current_char_list.append(char)
            continue

        if in_quote:
            current_char_list.append(char)
            # Se encontrou a aspa de fechamento correspondente
            if char == in_quote:
                in_quote = None
        else:
            if char == "'" or char == '"':
                in_quote = char
                current_char_list.append(char)
            elif char == ';':
                # ACHOU UM FIM DE COMANDO REAL
                cmd = "".join(current_char_list).strip()
                if cmd:
                    commands.append(cmd)
                current_char_list = []
            else:
                current_char_list.append(char)
    
    # Adiciona o último comando se houver (caso não termine com ;)
    cmd = "".join(current_char_list).strip()
    if cmd:
        commands.append(cmd)
        
    return commands

def clean_comments(content):
    """Remove linhas que são apenas comentários SQL (-- ou #)"""
    lines = content.split('\n')
    cleaned_lines = []
    for line in lines:
        stripped = line.strip()
        if not stripped or stripped.startswith('--') or stripped.startswith('#'):
            continue
        cleaned_lines.append(line) # Mantém a linha original (com indentação)
    return "\n".join(cleaned_lines)

# MELHORIA: questoes_teste movido para fora de execute_sql_file (era dado de negócio dentro de função de infraestrutura)
QUESTOES_TESTE = [
    "Quais as 5 operadoras com maior crescimento percentual de despesas entre o primeiro e o último trimestre analisado?  ",
    "Qual a distribuição de despesas por UF? Liste os 5 estados com maiores despesas totais.",
    "Quantas operadoras tiveram despesas acima da média geral em pelo menos 2 dos 3 trimestres analisados?"
]

def execute_sql_file(cursor, filename):
    filepath = os.path.join(SQL_DIR, filename)
    print(f"Lendo arquivo: {filename}...")
    
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            raw_content = f.read()
    except UnicodeDecodeError:
        with open(filepath, 'r', encoding='latin1') as f:
            raw_content = f.read()
    # Substitui os placeholders pelos caminhos reais calculados no início do script
    if '{{CAMINHO_RAW}}' in raw_content:
        print(f"   Injetando caminho RAW: {RAW_DIR_MYSQL}")
        raw_content = raw_content.replace('{{CAMINHO_RAW}}', RAW_DIR_MYSQL)
    
    if '{{CAMINHO_PROCESSED}}' in raw_content:
        print(f"   Injetando caminho PROCESSED: {PROCESSED_DIR_MYSQL}")
        raw_content = raw_content.replace('{{CAMINHO_PROCESSED}}', PROCESSED_DIR_MYSQL)
        
    # Fallback: Se o SQL ainda usar o antigo {{CAMINHO}}, assume que é processed
    if '{{CAMINHO}}' in raw_content:
        raw_content = raw_content.replace('{{CAMINHO}}', PROCESSED_DIR_MYSQL)

    # MELHORIA: substituição dinâmica de caminhos — elimina paths hardcoded no SQL
    data_raw = os.path.join(PROJECT_ROOT, "data", "raw").replace('\\', '/')
    data_processed = os.path.join(PROJECT_ROOT, "data", "processed").replace('\\', '/')
    raw_content = raw_content.replace('__DATA_RAW__', data_raw)
    raw_content = raw_content.replace('__DATA_PROCESSED__', data_processed)

    # Limpa comentários de linha inteira
    clean_content = clean_comments(raw_content)

    # Usa o separador inteligente
    commands = split_sql_statements(clean_content)
    
    print(f"   -> Encontrados {len(commands)} comandos.")
    contador_pergunta = 0
    #
    for command in commands:
        if not command.strip():
            continue
            
        try:
            # Tenta executar
            cursor.execute(command)
            
            # Se for SELECT ou WITH, mostra resultado
            if command.upper().startswith("SELECT") or command.upper().startswith("WITH"):
        # Tenta pegar a pergunta da lista, se acabar usa um título padrão
                if contador_pergunta < len(QUESTOES_TESTE):
                    titulo = QUESTOES_TESTE[contador_pergunta]
                else:
                    titulo = f"Resultado da Query Extra {contador_pergunta + 1}:"
                
                print(f"\n[QUESTÃO] {titulo}")
                
                results = cursor.fetchall()
                if not results:
                    print("      (Nenhum dado retornado)")
                else:
                    if cursor.description:
                        col_names = [i[0] for i in cursor.description]
                        print(f"Colunas: {col_names}")
                    
                    for row in results[:5]: 
                        print(f"         {row}")
                    
                    if len(results) > 5:
                        print(f"      ... (Total: {len(results)} linhas)")
                
                # Incrementa para a próxima query do arquivo
                contador_pergunta += 1
            
            # Confirmação visual para comandos pesados
            elif command.upper().startswith("LOAD DATA"):
                print(" Dados importados com sucesso.")

        except mysql.connector.Error as err:
            # Ignora erros de "já existe"
            if err.errno in (1050, 1007): 
                pass
            else:
                print(f"    Erro no comando: {command[:50]}...")
                print(f"      Mensagem: {err}")

def run_load():
    print(" Iniciando Carga no Banco de Dados...")
    
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        for sql_file in SQL_FILES:
            print(f"\n--- Executando {sql_file} ---")
            execute_sql_file(cursor, sql_file)
            
        print("\n Processo finalizado!")
        
    except mysql.connector.Error as err:
        print(f" Erro fatal de conexão: {err}")
    finally:
        if conn and conn.is_connected():
            cursor.close()
            conn.close()
            print("Conexão encerrada.")

if __name__ == "__main__":
    run_load()