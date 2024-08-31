import os

from dotenv import load_dotenv
from pymongo import MongoClient, errors

from models.Base import Base

load_dotenv()
## GEOJSON     split_rua = rua.upper().split(",")

def conectar_mongodb():
    try:
        from urllib.parse import quote_plus
        uri = "mongodb://%s:%s@%s" % (
            quote_plus(os.getenv('MONGO_USER')), quote_plus(os.getenv('MONGO_PASSWORD')), os.getenv('MONGO_HOST'))

        #uri_local = "mongodb://localhost:27017/"
        cliente = MongoClient(uri)
        db = cliente['gogood']
        print("Conexão ao MongoDB bem-sucedida.")
        return db
    except errors.ConnectionFailure as e:
        print("Erro ao conectar ao MongoDB:", e)
        return None

def excluir_ocorrencias_ano(ano, db):
    colecao = db['ocorrencias-detalhadas']

    colecao.delete_many({"ano": ano})
def inserir_mongo(insert, db):
    colecao = db['ocorrencias-detalhadas']


    colecao.insert_one(insert)
