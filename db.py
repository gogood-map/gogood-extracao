import os

from dotenv import load_dotenv
from pymongo import MongoClient, errors
from models.Base import Base

load_dotenv()
## GEOJSON     split_rua = rua.upper().split(",")
class Db:
    db: any

    def __init__(self):
        self.db = self.conectar_mongodb()
    def conectar_mongodb(self):
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

    def excluir_ocorrencias_ano(self,ano):
        colecao = self.db['ocorrencias-detalhadas']

        colecao.delete_many({"ano": int(ano)})
    def inserir_mongo(self, insert):
        colecao = self.db['ocorrencias-detalhadas']
        colecao.insert_one(insert)

    def buscar_documento_unico(self, colecao, query):
        colecao_banco = self.db[colecao]
        doc = colecao_banco.find_one(query)
        return doc


