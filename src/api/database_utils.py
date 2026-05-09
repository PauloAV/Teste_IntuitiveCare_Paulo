import mysql.connector
import os
from dotenv import load_dotenv
from fastapi import HTTPException

load_dotenv()

def get_database_connection():
    
    try:
        connection = mysql.connector.connect(
            host=os.getenv('DB_HOST'),
            user=os.getenv('DB_USER'),
            password=os.getenv('DB_PASSWORD'),
            database=os.getenv('DB_NAME')
        )
        return connection
    except mysql.connector.Error as err:
        # BUG CORRIGIDO: antes retornava None, causando AttributeError em todos os endpoints
        raise HTTPException(status_code=503, detail=f"Falha na conexão com o banco de dados: {err}")