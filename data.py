import gc
from datetime import datetime

import pandas
from pandas import DataFrame, Series
from unidecode import unidecode
import asyncio
from geo import reverter_coordenada_em_endereco
from models.Db import Db
from models.Ocorrencia import Ocorrencia

db = Db('ocorrencias-detalhadas')
ano_base = 0


async def ler_base_excel(caminho_arquivo: str, ano: int):
    global ano_base
    definir_ano(ano)
    print(ano_base)
    print("Lendo Excel de Ocorrências")

    arquivo = pandas.ExcelFile(caminho_arquivo)
    abas = []
    for aba in arquivo.sheet_names:
        abas.append(aba)
    aba_extracao = None
    if len(abas) == 1:
        aba_extracao = abas[0]
    else:
        aba_extracao = abas[1]

    df_base = pandas.read_excel(caminho_arquivo, sheet_name=aba_extracao,
                                dtype={'LATITUDE': str, 'LONGITUDE': str, 'NUM_BO': str, 'DATA_OCORRENCIA_BO': str})

    ocorrencias_vias_publicas = tratar_base(df_base)
    await inserir_dados(ocorrencias_vias_publicas)
    gc.collect()


def tratar_base(df: DataFrame):
    index_coordenadas_invalidas = df.query(
        'LATITUDE.isnull() | LONGITUDE.isnull() | LATITUDE == "0" | LONGITUDE == "0"',
        engine='python').index

    df.drop(index_coordenadas_invalidas, axis=0, inplace=True)

    df.drop(columns=[
        "NOME_DEPARTAMENTO",
        "NOME_SECCIONAL",
        "DATA_COMUNICACAO",
        "NUMERO_LOGRADOURO",
        "NOME_DELEGACIA_CIRCUNSCRIÇÃO",
        "NOME_DEPARTAMENTO_CIRCUNSCRIÇÃO",
        "NOME_SECCIONAL_CIRCUNSCRIÇÃO",
        "NOME_MUNICIPIO_CIRCUNSCRIÇÃO",
        "RUBRICA",
        "DESCR_CONDUTA",
        "MES_ESTATISTICA",
        "ANO_ESTATISTICA",
    ], axis=1, inplace=True)

    ocorrencias_vias_publicas = df.query(
        'DESCR_TIPOLOCAL == "Via Pública" | DESCR_TIPOLOCAL == "Ciclofaixa" | DESCR_TIPOLOCAL == "Praça"')
    return ocorrencias_vias_publicas


async def inserir_dados(df: DataFrame):
    await db.excluir({'ano': ano_base})
    for index, o in df.iterrows():
        ocorrencia = await tratar_ocorrencia(o)
        geojson = {'type': "Point", 'coordinates': [float(ocorrencia.lng), float(ocorrencia.lat)]}
        insercao = {
            'num_bo': ocorrencia.num_bo,
            'localizacao': geojson,
            'crime': ocorrencia.crime,
            'tipo_local': ocorrencia.local,
            'ano': ocorrencia.ano,
            'rua': ocorrencia.rua,
            'bairro': ocorrencia.bairro,
            'delegacia': ocorrencia.delegacia,
            'cidade': ocorrencia.cidade,
            'data_ocorrencia': ocorrencia.data,
            'periodo': ocorrencia.periodo
        }
        await db.inserir(insercao)


async def tratar_ocorrencia(registro: Series):
    ocorrencia = Ocorrencia(
        registro["ANO_BO"],
        registro["NUM_BO"],
        registro["DESCR_TIPOLOCAL"],
        "{}".format(registro["LOGRADOURO"]).split(",")[0],
        registro["LATITUDE"],
        registro["LONGITUDE"],
        registro["NATUREZA_APURADA"],
        registro['DESC_PERIODO'],
        registro["BAIRRO"],
        registro["CIDADE"],
        registro['DATA_OCORRENCIA_BO'],
        registro['NOME_DELEGACIA']
    )
    horario_bo = registro['HORA_OCORRENCIA_BO']
    query_cidade_bairro = {
        'cidade': normalizar(ocorrencia.cidade),
        'bairro': normalizar(ocorrencia.bairro)
    }

    busca_cidade_bairro = await db.buscar_unico(query_cidade_bairro)

    if busca_cidade_bairro:
        query_rua = {'rua': normalizar(ocorrencia.rua)}
        doc_rua = await db.buscar_unico(query_rua)

        if doc_rua is None:
            ocorrencia = await buscar_informacoes_endereco_ocorrencia(ocorrencia)
        else:
            ocorrencia.rua = normalizar(ocorrencia.rua)
            ocorrencia.bairro = normalizar(ocorrencia.bairro)
            ocorrencia.cidade = normalizar(ocorrencia.cidade)

    else:
        ocorrencia = await buscar_informacoes_endereco_ocorrencia(ocorrencia)

    if (ocorrencia.periodo is None or ocorrencia.periodo == "Em hora incerta") and horario_bo is not None and horario_bo != '':
        try:
            ocorrencia.periodo = definir_periodo(horario_bo)
        except:
            ocorrencia.periodo = ''

    return ocorrencia


def definir_periodo(hora):
    horario = datetime.strptime("{}".format(hora), "%H:%M:%S")

    madrugada = horario.replace(hour=0, minute=0)
    manha = horario.replace(hour=5, minute=59)
    tarde = horario.replace(hour=12, minute=0)
    noite = horario.replace(hour=19, minute=0)

    if horario >= madrugada and horario < manha:
        return "Madrugada"
    elif horario >= manha and horario < tarde:
        return "Manhã"
    elif horario >= tarde and horario < noite:
        return "Tarde"
    else:
        return "Noite"


async def buscar_informacoes_endereco_ocorrencia(ocorrencia):
    busca = asyncio.create_task(reverter_coordenada_em_endereco(ocorrencia.lat, ocorrencia.lng))
    rua, bairro, cidade = await busca

    ocorrencia.rua = normalizar(rua) if rua != "" else ocorrencia.rua
    ocorrencia.bairro = normalizar(bairro) if bairro != "" else ocorrencia.bairro
    ocorrencia.cidade = normalizar(cidade) if cidade != "" else ocorrencia.cidade

    if ocorrencia.cidade == "S.PAULO":
        ocorrencia.cidade = "SAO PAULO"
    return ocorrencia


def normalizar(texto):
    texto = "{}".format(texto)
    return "{}".format(unidecode(texto)).upper().strip()


def definir_ano(ano):
    global ano_base
    ano_base = ano
