from pymongo import MongoClient, errors
import mysql.connector
import pyodbc
import pymysql
from models.Base import Base
from models.Ocorrencia import Ocorrencia
from web import obter_rua_coordenada
from dotenv import load_dotenv
import os

load_dotenv()


def conectar_mongodb():
    try:
        cliente = MongoClient(os.getenv("MONGO_URL"))
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
        charset='utf8'
    )

    return conexao.cursor()


def inserir_mongo(ocorrencias, ano):
    db = conectar_mongodb()
    colecao = db['ocorrencias-detalhadas']
    mongo_inserts = []
    for index, o in ocorrencias.iterrows():
        ocorrencia = Ocorrencia(
            o["ANO_BO"], o["NUM_BO"], o["RUA"], o["DESCR_TIPOLOCAL"], o["LATITUDE"],
            o["LONGITUDE"], o["CRIME"]
        )

        rua: str = obter_rua_coordenada(ocorrencia.lat, ocorrencia.lng)
        if rua == "": rua = ocorrencia.rua

        insert_mongo = {'lat': ocorrencia.lat,
                        'lng': ocorrencia.lng,
                        'crime': ocorrencia.crime,
                        'ano': ocorrencia.ano,
                        'rua': rua.upper()
                        }
        mongo_inserts.append(insert_mongo)

    colecao.insert_many(mongo_inserts)


def inserir_mysql(ocorrencias, ano):
    cursor = conectar_mysql()

    cursor.execute("DELETE FROM ocorrencias WHERE ano_ocorrencia = {};".format(ano))
    i = 0
    mysql_inserts = "INSERT INTO ocorrencias VALUES "
    for index, o in ocorrencias.iterrows():
        ocorrencia = Ocorrencia(
            o["ANO_BO"], o["NUM_BO"], o["RUA"], o["DESCR_TIPOLOCAL"], o["LATITUDE"],
            o["LONGITUDE"], o["CRIME"]
        )
        insert_mysql = "(default, {}, {},{})".format(ocorrencia.lat, ocorrencia.lng, ocorrencia.ano)
        insert_mysql+=","
        mysql_inserts += insert_mysql
        if i == 100:
            cursor.execute(mysql_inserts[:-1])
            mysql_inserts = "INSERT INTO ocorrencias VALUES "
            i = 0
        i += 1


def cadastrar(banco, ocorrencias, base_escolhida: Base):
    if banco == "MONGODB":
        inserir_mongo(ocorrencias, base_escolhida.ano_base)
    elif banco == "MYSQL":
        inserir_mysql(ocorrencias, base_escolhida.ano_base)
    else:
        inserir_mysql(ocorrencias, base_escolhida.ano_base)
        inserir_mongo(ocorrencias, base_escolhida.ano_base)
