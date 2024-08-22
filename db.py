import os

import pymysql
from dotenv import load_dotenv
from pymongo import MongoClient, errors
from unidecode import unidecode

from models.Base import Base
from models.Ocorrencia import Ocorrencia
from web import obter_endereco_por_coordenada

load_dotenv()
## GEOJSON     split_rua = rua.upper().split(",")


def conectar_mongodb():
    try:

        from urllib.parse import quote_plus

        uri = "mongodb://%s:%s@%s" % (
            quote_plus(os.getenv('MONGO_USER')), quote_plus(os.getenv('MONGO_PASSWORD')), os.getenv('MONGO_HOST'))


        cliente = MongoClient(uri)
        db = cliente['gogood']
        print("Conexão ao MongoDB bem-sucedida.")
        return db
    except errors.ConnectionFailure as e:
        print("Erro ao conectar ao MongoDB:", e)
        return None


def conectar_mysql():
    conexao = pymysql.connect(
        host=os.getenv("MYSQL_URL"),
        user=os.getenv("MYSQL_USERNAME"),
        password=os.getenv("MYSQL_PASSWORD"),
        port=3306,
        db='GoGood',
    )


    return conexao


def inserir_mongo(ocorrencias:list[Ocorrencia] , ano):
    db = conectar_mongodb()
    colecao = db['ocorrencias-detalhadas']

    colecao.delete_many({"ano":ano})
    mongo_inserts = []
    for ocorrencia in ocorrencias:

        rua: str = obter_endereco_por_coordenada(ocorrencia.lat, ocorrencia.lng)
        if rua == "": rua = ocorrencia.rua

        insert_mongo = {'lat': ocorrencia.lat,
                        'lng': ocorrencia.lng,
                        'crime': ocorrencia.crime,
                        'ano': ocorrencia.ano,
                        'rua':  unidecode(rua),
                        'bairro': ocorrencia.bairro,
                        'delegacia':ocorrencia.delegacia,
                        'cidade': ocorrencia.cidade,
                        'data_ocorrencia':o['DATA_OCORRENCIA_BO'],
                        'periodo': o['DESC_PERIODO']
                        }
        mongo_inserts.append(insert_mongo)

    colecao.insert_many(mongo_inserts)


def inserir_mysql(ocorrencias:list[Ocorrencia], ano):
    conexao = conectar_mysql()
    cursor = conexao.cursor()
    cursor.execute("DELETE FROM ocorrencias WHERE ano_ocorrencia = {};".format(ano))
    conexao.commit()
    i = 0
    mysql_inserts = "INSERT INTO ocorrencias VALUES "
    for o in ocorrencias:

        insert_mysql = "(default, {}, {},{}),".format(o.lng, o.lat, ano)
        mysql_inserts += insert_mysql
    try:
        cursor.execute(mysql_inserts[:-1])
        conexao.commit()
        conexao.close()

        print("Dados inseridos")
    except Exception as e:
        print("Houve um erro"+e)

def cadastrar(banco, ocorrencias, base_escolhida: Base):
    if banco == banco.MONGO:
        inserir_mongo(ocorrencias, base_escolhida.ano_base)
    elif banco == banco.MYSQL:
        inserir_mysql(ocorrencias, base_escolhida.ano_base)
    else:
        inserir_mysql(ocorrencias, base_escolhida.ano_base)
        inserir_mongo(ocorrencias, base_escolhida.ano_base)
