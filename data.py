import gc
from datetime import datetime

import pandas
from pandas import DataFrame, Series
from unidecode import unidecode
import asyncio
from geo import reverter_coordenada_em_endereco
from models.Db import Db
from models.Enderecos import Enderecos
from models.Ocorrencia import Ocorrencia

db = Db('ocorrencias-detalhadas')
ano_base = 0

df: DataFrame = None


async def ler_base(caminho_arquivo: str, ano: int, enderecos: Enderecos):
    global ano_base
    global df
    ano_base = ano

    print(ano_base)
    print("Lendo Base de Ocorrências")

    df_base = pandas.read_csv(caminho_arquivo, sep=";",
                              dtype={'LATITUDE': str, 'LONGITUDE': str, 'NUM_BO': str, 'DATA_OCORRENCIA_BO': str,
                                     'DATA_COMUNICACAO': str, "ANO_BO": int}, low_memory=False)

    df_base['DATA_COMUNICACAO'] = pandas.to_datetime(df_base['DATA_COMUNICACAO'], errors='coerce', dayfirst=True)

    df_base['DATA_OCORRENCIA_BO'] = pandas.to_datetime(df_base['DATA_OCORRENCIA_BO'], errors='coerce', dayfirst=True)
    print(f"A Base contém {df_base['NUM_BO'].shape[0]} registros não tratados.")
    print("Tratando dados...")
    df_base = tratar_base(df_base)

    await pre_insercao_ocorrencias(df_base, enderecos)


def tratar_base(df_sem_tratamento: DataFrame):
    ocorrencias_invalidas = df_sem_tratamento.query(
        f"LATITUDE.isnull() | LONGITUDE.isnull() | LATITUDE == '0' | LONGITUDE == '0' or DATA_OCORRENCIA_BO < '{ano_base}-01-01' or DATA_OCORRENCIA_BO > '{ano_base}-12-31'",
        engine='python').index

    df_sem_tratamento.drop(ocorrencias_invalidas, axis=0, inplace=True)
    df_sem_tratamento["LATITUDE"] = df_sem_tratamento["LATITUDE"].replace(",", ".", regex=True)
    df_sem_tratamento["LONGITUDE"] = df_sem_tratamento["LONGITUDE"].replace(",", ".", regex=True)

    df_sem_tratamento = df_sem_tratamento.astype(
        {
            'DATA_OCORRENCIA_BO': str,
            'DATA_COMUNICACAO': str
        }
    )

    del ocorrencias_invalidas

    gc.collect()
    df_sem_tratamento.drop(columns=[
        "NOME_DEPARTAMENTO",
        "NOME_SECCIONAL",
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

    df_sao_paulo = df_sem_tratamento[df_sem_tratamento['CIDADE'] == "S.PAULO"].sort_values(['BAIRRO', 'LOGRADOURO'],
                                                                                           ascending=[True, True])
    df_outros_municipios = df_sem_tratamento[df_sem_tratamento['CIDADE'] != "S.PAULO"].sort_values(
        ['CIDADE', 'BAIRRO', 'LOGRADOURO'],
        ascending=[True, True, True])

    df_consolidado = pandas.concat([df_sao_paulo, df_outros_municipios], ignore_index=True)

    ocorrencias_vias_publicas = df_consolidado.query(
        'DESCR_TIPOLOCAL == "Via Pública" | DESCR_TIPOLOCAL == "Ciclofaixa" | DESCR_TIPOLOCAL == "Praça"')

    print(f"Serão inseridos {ocorrencias_vias_publicas['NUM_BO'].shape[0]} registros tratados.")
    return ocorrencias_vias_publicas


async def pre_insercao_ocorrencias(df_tratado: DataFrame, enderecos: Enderecos):
    global df
    df = df_tratado

    df['contagem_ocorrencias'] = df.groupby(['LOGRADOURO', 'BAIRRO', 'CIDADE'])['LOGRADOURO'].transform('count')
    df.sort_values(['contagem_ocorrencias'], inplace=True, ascending=[False])
    df.reset_index(drop=True, inplace=True)

    tamanho_df = len(df)
    i = 0
    while i < tamanho_df:
        o = df.loc[i]
        i += 1

        cidade_antes_tratamento = o['CIDADE']
        bairro_antes_tratamento = o['BAIRRO']
        logradouro_antes_tratamento = o['LOGRADOURO']

        ocorrencia = await tratar_ocorrencia(o, enderecos)

        if ocorrencia.rua not in enderecos.logradouros:
            enderecos.logradouros.append(ocorrencia.rua)
        if ocorrencia.bairro not in enderecos.bairros:
            enderecos.bairros.append(ocorrencia.bairro)
        if ocorrencia.cidade not in enderecos.cidades:
            enderecos.cidades.append(ocorrencia.cidade)

        df.loc[df["LOGRADOURO"] == logradouro_antes_tratamento, 'LOGRADOURO'] = ocorrencia.rua
        df.loc[df["BAIRRO"] == bairro_antes_tratamento, 'BAIRRO'] = ocorrencia.bairro
        df.loc[df["CIDADE"] == cidade_antes_tratamento, 'CIDADE'] = ocorrencia.cidade

        df_ocorrencias_mesmo_endereco = df.query(
            'LOGRADOURO == "{}" & BAIRRO == "{}" & CIDADE == "{}"'.format(
                ocorrencia.rua.replace("'", "").replace('"', ""),
                ocorrencia.bairro.replace("'", "").replace('"', ""),
                ocorrencia.cidade.replace("'", "").replace('"', "")
            ))

        indices_exclusao = df_ocorrencias_mesmo_endereco.index
        df.drop(indices_exclusao, inplace=True, axis=0)

        df_ocorrencias_mesmo_endereco.reset_index(drop=True, inplace=True)
        df.reset_index(drop=True, inplace=True)
        tamanho_df = len(df)

        await inserir_ocorrencias(df_ocorrencias_mesmo_endereco)


async def inserir_ocorrencias(df_insercao: DataFrame):
    i = 0
    tamanho_df = len(df_insercao)

    lista_insercao = []

    while i < tamanho_df:
        lista_insercao.append(transformar_linha_em_ocorrencia(df_insercao.loc[i]).converter_em_documento())
        i += 1

    await db.inserir_lista(lista_insercao)


async def tratar_ocorrencia(registro: Series, enderecos: Enderecos):
    ocorrencia = transformar_linha_em_ocorrencia(registro)

    rua_normalizada = normalizar(ocorrencia.rua)
    bairro_normalizado = normalizar(ocorrencia.bairro)
    cidade_normalizada = normalizar(ocorrencia.cidade)

    query_cidade_bairro = {
        'cidade': cidade_normalizada,
        'bairro': bairro_normalizado
    }

    if bairro_normalizado not in enderecos.bairros and cidade_normalizada not in enderecos.cidades or rua_normalizada not in enderecos.logradouros:

        busca_cidade_bairro = await db.buscar_unico(query_cidade_bairro)
        if busca_cidade_bairro:
            query_rua = {'rua': rua_normalizada}
            doc_rua = await db.buscar_unico(query_rua)

            if doc_rua is None:
                ocorrencia = await buscar_informacoes_endereco_ocorrencia(ocorrencia)
            else:
                ocorrencia.rua = rua_normalizada
                ocorrencia.bairro = bairro_normalizado
                ocorrencia.cidade = cidade_normalizada
        else:
            ocorrencia = await buscar_informacoes_endereco_ocorrencia(ocorrencia)
    else:
        ocorrencia.rua = rua_normalizada
        ocorrencia.bairro = bairro_normalizado
        ocorrencia.cidade = cidade_normalizada

    return ocorrencia


async def buscar_informacoes_endereco_ocorrencia(ocorrencia):
    busca = asyncio.create_task(reverter_coordenada_em_endereco(ocorrencia.lat, ocorrencia.lng))
    rua, bairro, cidade = await busca

    ocorrencia.rua = normalizar(rua) if rua != "" else normalizar(ocorrencia.rua)
    ocorrencia.bairro = normalizar(bairro) if bairro != "" else normalizar(ocorrencia.bairro)
    ocorrencia.cidade = normalizar(cidade) if cidade != "" else normalizar(ocorrencia.cidade)

    if ocorrencia.cidade == "S.PAULO":
        ocorrencia.cidade = "SAO PAULO"
    return ocorrencia


def transformar_linha_em_ocorrencia(registro: Series):
    global ano_base
    ocorrencia = Ocorrencia(
        registro["ANO_BO"].item(),
        registro["NUM_BO"],
        registro["DESCR_TIPOLOCAL"],
        "{}".format(registro["LOGRADOURO"]).replace("'", "").replace('"', "").split(",")[0],
        registro["LATITUDE"],
        registro["LONGITUDE"],
        registro["NATUREZA_APURADA"],
        registro["BAIRRO"],
        registro["CIDADE"],
        registro['DATA_OCORRENCIA_BO'],
        registro['NOME_DELEGACIA'],
        registro['DATA_COMUNICACAO']
    )
    return ocorrencia


def normalizar(texto):
    texto = "{}".format(texto)
    texto = texto.replace('"', "")
    texto = texto.replace("'", "")
    return "{}".format(unidecode(texto)).upper().strip()
