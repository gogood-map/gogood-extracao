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


async def ler_base_excel(caminho_arquivo: str, ano: int, enderecos: Enderecos):
    global ano_base
    definir_ano(ano)
    print(ano_base)
    print("Lendo Base de Ocorrências")

    df_base = pandas.read_csv(caminho_arquivo, sep=";",
                              dtype={'LATITUDE': str, 'LONGITUDE': str, 'NUM_BO': str, 'DATA_OCORRENCIA_BO': str,
                                     'DATA_COMUNICACAO': str, "ANO_BO": int}, low_memory=False)

    df_base['DATA_COMUNICACAO'] = pandas.to_datetime(df_base['DATA_COMUNICACAO'], errors='coerce', dayfirst=True)

    df_base['DATA_OCORRENCIA_BO'] = pandas.to_datetime(df_base['DATA_OCORRENCIA_BO'], errors='coerce', dayfirst=True)

    print(f"A Base contém {df_base['NUM_BO'].shape[0]} registros não tratados.")
    print("Tratando dados...")
    df_ocorrencias_vias_publicas = tratar_base(df_base)

    await inserir_dados(df_ocorrencias_vias_publicas, enderecos)


def tratar_base(df: DataFrame):
    ocorrencias_invalidas = df.query(
        f"LATITUDE.isnull() | LONGITUDE.isnull() | LATITUDE == '0' | LONGITUDE == '0' or DATA_OCORRENCIA_BO < '{ano_base}-01-01' or DATA_OCORRENCIA_BO > '{ano_base}-12-31'",
        engine='python').index

    df.drop(ocorrencias_invalidas, axis=0, inplace=True)
    df["LATITUDE"] = df["LATITUDE"].replace(",", ".", regex=True)
    df["LONGITUDE"] = df["LONGITUDE"].replace(",", ".", regex=True)

    df = df.astype(
        {
            'DATA_OCORRENCIA_BO': str,
            'DATA_COMUNICACAO': str
        }
    )

    del ocorrencias_invalidas

    gc.collect()
    print(df.info())
    df.drop(columns=[
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

    df_sao_paulo = df[df['CIDADE'] == "S.PAULO"].sort_values(['BAIRRO', 'LOGRADOURO'], ascending=[True, True])
    df_outros_municipios = df[df['CIDADE'] != "S.PAULO"].sort_values(['CIDADE', 'BAIRRO', 'LOGRADOURO'],
                                                                     ascending=[True, True, True])

    df_consolidado = pandas.concat([df_sao_paulo, df_outros_municipios], ignore_index=True)

    ocorrencias_vias_publicas = df_consolidado.query(
        'DESCR_TIPOLOCAL == "Via Pública" | DESCR_TIPOLOCAL == "Ciclofaixa" | DESCR_TIPOLOCAL == "Praça"')

    print(f"Serão inseridos {ocorrencias_vias_publicas['NUM_BO'].shape[0]} registros tratados.")
    return ocorrencias_vias_publicas


async def inserir_dados(df: DataFrame, enderecos: Enderecos):

    df['contagem_ocorrencias'] = df.groupby(['LOGRADOURO', 'BAIRRO', 'CIDADE'])['LOGRADOURO'].transform('count')
    df.sort_values(['contagem_ocorrencias'], inplace=True, ascending=[False])
    df.reset_index(drop=True, inplace=True)

    ultimo_documento = await db.buscar_ultimo_inserido()

    if ultimo_documento is not None:
        indice_ultimo_bo = df.query('NUM_BO == "{}"'.format(ultimo_documento['num_bo'])).index.values.max()
        df = df.drop(df.index[0:indice_ultimo_bo+1])
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
                ocorrencia.rua,
                ocorrencia.bairro,
                ocorrencia.cidade
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
        lista_insercao.append(converter_linha_documento(df_insercao.loc[i]))
        i += 1

    await db.inserir_lista(lista_insercao)


def converter_linha_documento(linha: Series):
    ocorrencia = Ocorrencia(
        ano_base,
        linha["NUM_BO"],
        linha["DESCR_TIPOLOCAL"],
        "{}".format(linha["LOGRADOURO"]).split(",")[0],
        linha["LATITUDE"],
        linha["LONGITUDE"],
        linha["NATUREZA_APURADA"],
        linha["BAIRRO"],
        linha["CIDADE"],
        linha['DATA_OCORRENCIA_BO'],
        linha['NOME_DELEGACIA'],
        linha['DATA_COMUNICACAO']
    )

    geojson = {'type': "Point", 'coordinates': [float(ocorrencia.lng), float(ocorrencia.lat)]}
    documento = {
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
        'data_abertura_bo': ocorrencia.data_bo,
    }
    return documento


async def tratar_ocorrencia(registro: Series, enderecos: Enderecos):
    ocorrencia = Ocorrencia(
        ano_base,
        registro["NUM_BO"],
        registro["DESCR_TIPOLOCAL"],
        "{}".format(registro["LOGRADOURO"]).split(",")[0],
        registro["LATITUDE"],
        registro["LONGITUDE"],
        registro["NATUREZA_APURADA"],
        registro["BAIRRO"],
        registro["CIDADE"],
        registro['DATA_OCORRENCIA_BO'],
        registro['NOME_DELEGACIA'],
        registro['DATA_COMUNICACAO']
    )

    query_cidade_bairro = {
        'cidade': normalizar(ocorrencia.cidade),
        'bairro': normalizar(ocorrencia.bairro)
    }

    if (normalizar(ocorrencia.bairro) not in enderecos.bairros and normalizar(
            ocorrencia.cidade) not in enderecos.cidades
            or normalizar(ocorrencia.rua) not in enderecos.logradouros):
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
    else:
        ocorrencia.rua = normalizar(ocorrencia.rua)
        ocorrencia.bairro = normalizar(ocorrencia.bairro)
        ocorrencia.cidade = normalizar(ocorrencia.cidade)

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
    texto = texto.replace('"', "")
    texto = texto.replace("'", "")
    return "{}".format(unidecode(texto)).upper().strip()


def definir_ano(ano):
    global ano_base
    ano_base = ano
