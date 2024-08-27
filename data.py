import gc
from datetime import datetime

import pandas
from unidecode import unidecode

from models.Ocorrencia import Ocorrencia


def ler_base_excel(caminho_arquivo: str, ano: int):
    global df_apoio
    tem_segundo_semestre = False

    if ano != datetime.now().year:
        planilhas = [1]
    else:
        planilhas = [0, 1]

    try:
        df_base = pandas.read_excel(caminho_arquivo, sheet_name=planilhas,
                                    dtype={'LATITUDE': str, 'LONGITUDE': str, 'NUM_BO': str})
        tem_segundo_semestre = True
    except:
        df_base = pandas.read_excel(caminho_arquivo, sheet_name=[0],
                                    dtype={'LATITUDE': str, 'LONGITUDE': str, 'NUM_BO': str})
        tem_segundo_semestre = False

    if ano == datetime.now().year and tem_segundo_semestre:
        df_base = pandas.concat([df_base[0], df_base[1]])
    else:
        if ano == datetime.now().year:
            df_apoio = df_base[0]
        else:
            df_apoio = df_base[1]

        df_base = df_apoio.copy()
        del df_apoio
        gc.collect()

    coordenadas_invalidas = df_base.query('LATITUDE.isnull() | LONGITUDE.isnull() | LATITUDE == "0" | LONGITUDE == "0"',
                                          engine='python').index

    df_base.drop(coordenadas_invalidas, axis=0, inplace=True)

    del coordenadas_invalidas
    gc.collect()

    df_base.drop(columns=[
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

    ocorrencias_vias_publicas = df_base.copy().query(
        'DESCR_TIPOLOCAL == "Via Pública" | DESCR_TIPOLOCAL == "Ciclofaixa" | DESCR_TIPOLOCAL == "Praça"')

    del df_base
    gc.collect()

    ocorrencias_vias_publicas.sort_values(by='LATITUDE', inplace=True, kind="quicksort")
    ocorrencias_final: list[Ocorrencia] = []

    for index, o in ocorrencias_vias_publicas.iterrows():
        ocorrencia = Ocorrencia(
            o["ANO_BO"],
            o["NUM_BO"],
            o["DESCR_TIPOLOCAL"],
            o["LOGRADOURO"],
            o["LATITUDE"],
            o["LONGITUDE"],
            o["NATUREZA_APURADA"],
            o['DESC_PERIODO'],
            o["BAIRRO"],
            o["CIDADE"],
            o['DATA_OCORRENCIA_BO'],
            o['NOME_DELEGACIA']
        )

        ocorrencia.rua = "{}".format(o["LOGRADOURO"]).strip()
        ocorrencia.bairro = "{}".format(o["BAIRRO"]).strip()
        ocorrencia.cidade = "{}".format(o["CIDADE"]).strip()

        if ocorrencia.bairro is not None:
            bairro = unidecode(ocorrencia.bairro.upper())
        if ocorrencia.cidade is not None:
            cidade = unidecode(ocorrencia.cidade.upper())

        if ocorrencia.cidade == "S.PAULO": cidade = "SAO PAULO"

        if (ocorrencia.periodo is None or ocorrencia == "Em hora incerta") and o['HORA_OCORRENCIA_BO'] is not None:
            ocorrencia.periodo = definir_periodo(o['HORA_OCORRENCIA_BO'])

        ocorrencias_final.append(ocorrencia)

    hoje = datetime.now()
    ocorrencias_vias_publicas.to_csv(f"./backups/dados_tratados_ano_{ano}_{hoje.strftime('%Y_%m_%d')}.csv", sep=';',
                                     encoding='utf-8',
                                     index=False)

    del ocorrencias_vias_publicas
    gc.collect()
    return ocorrencias_final


def ler_csv(caminho_arquivo):
    ocorrencias_final: list[Ocorrencia] = []
    df_csv = pandas.read_csv(caminho_arquivo, sep=";", dtype={'LATITUDE': str, 'LONGITUDE': str, 'NUM_BO': str})

    for i, o in df_csv.iterrows():
        ocorrencia = Ocorrencia(
            o["ANO_BO"],
            o["NUM_BO"],
            o["DESCR_TIPOLOCAL"],
            o["LOGRADOURO"],
            o["LATITUDE"],
            o["LONGITUDE"],
            o["NATUREZA_APURADA"],
            o['DESC_PERIODO'],
            o["BAIRRO"],
            o["CIDADE"],
            o['DATA_OCORRENCIA_BO'],
            o['NOME_DELEGACIA']
        )
        ocorrencias_final.append(ocorrencia)

    return ocorrencias_final


def definir_periodo(hora):
    horario = datetime.strptime(hora, "%H:%M:%S")

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
