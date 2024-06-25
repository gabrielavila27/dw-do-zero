#imports
import yfinance as yf
import pandas as pd 
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os 
# import requests

load_dotenv()

#imports varavei de ambiente
DB_HOST=os.getenv('DB_HOST_PROD')
DB_PORT=os.getenv('DB_PORT_PROD')
DB_NAME=os.getenv('DB_NAME_PROD')
DB_USER=os.getenv('DB_USER_PROD')
DB_PASS=os.getenv('DB_PASS_PROD')
DB_SCHEMA=os.getenv('DB_SCHEMA_PROD')

DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

engine = create_engine(DATABASE_URL)

#funções
#pegar a cotação dos ATIVOS na bolsa
commodities = ['CL=F', 'GC=F', 'SI=F']

def buscar_dados_commodities(simbolo, periodo='5d', intervalo='1d'): 
    # response = requests.get("url...")
    ticker = yf.Ticker(simbolo)
    #retorna um DF  (Close = dados do fechamento)
    dados = ticker.history(period = periodo, interval = intervalo)[['Close']]
    dados['simbolo'] = simbolo
    return dados

#concatenar os ativos (1...2...3...) -> 1
def buscar_todos_dados_commodities(commodities): 
    todos_dados = []
    for simbolo in commodities: 
        dados = buscar_dados_commodities(simbolo)
        todos_dados.append(dados)
    return pd.concat(todos_dados)

#Salvar no banco de dados
def salvar_postgres(df, schema='public'):
    df.to_sql('commodities', engine, if_exists='replace', index=True, index_label='Date', schema=schema)
    #nome da tabela que vai pro banco, bacno a conectar, replace ou append da tabela, se quer colocar index na tabela, o que será o index da tabela 
    #Função do pandas para salvar o df diretamente no banco de dados


#debug


dados_concatenados = buscar_todos_dados_commodities(commodities)
salvar_postgres(dados_concatenados, schema='public')
# print(dados_concatenados)