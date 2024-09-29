import os

from dotenv import load_dotenv
from pymongo import errors
import motor.motor_asyncio


class Db:
    db: any
    colecao: any

    def __init__(self, nome_colecao: str):
        self.db = self.conectar()
        self.colecao = self.db[nome_colecao]

    def conectar(self):
        load_dotenv()
        try:
            from urllib.parse import quote_plus
            uri = "mongodb://%s:%s@%s" % (
                quote_plus(os.getenv('MONGO_USER')), quote_plus(os.getenv('MONGO_PASSWORD')), os.getenv('MONGO_HOST'))

            uri_local = "mongodb://localhost:27017/"
            cliente = motor.motor_asyncio.AsyncIOMotorClient(uri_local)
            db = cliente['gogood']
            print("Conexão ao MongoDB bem-sucedida.")
            return db
        except errors.ConnectionFailure as e:
            print("Erro ao conectar ao MongoDB:", e)
            return None

    def mudar_colecao(self, nome_colecao: str):
        self.colecao = self.db[nome_colecao]

    async def excluir(self, query):
        await self.colecao.delete_many(query)

    async def inserir(self, insert):
        await self.colecao.insert_one(insert)

    async def inserir_lista(self, insert):
        await self.colecao.insert_many(insert)

    async def buscar_max(self, campo):
        return await self.colecao.find_one(sort=[(campo, -1)])

    async def buscar(self, query):
        return await self.colecao.find(query)

    async def buscar_unico(self, query):
        return await self.colecao.find_one(query)

    async def buscar_ultimo_inserido(self):
        return await self.colecao.find_one({}, sort=[('_id', -1)])
