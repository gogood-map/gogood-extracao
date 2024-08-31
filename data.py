import gc
from datetime import datetime
import pandas
from unidecode import unidecode
from geo import reverter_coordenada_em_endereco
from models.Ocorrencia import Ocorrencia
from db import Db

colunas_drop = [
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
]


def ler_base_excel(caminho_arquivo: str, ano: int):
    from main import hoje
    db = Db()


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

    ocorrencias_vias_publicas = df_base.query(
        'DESCR_TIPOLOCAL == "Via Pública" | DESCR_TIPOLOCAL == "Ciclofaixa" | DESCR_TIPOLOCAL == "Praça"')
    ocorrencias_vias_publicas = ocorrencias_vias_publicas.sort_values('LOGRADOURO', axis=0)
    ocorrencias_vias_publicas['CIDADE'] = ocorrencias_vias_publicas['CIDADE'].str.replace('S.PAULO', 'SAO PAULO')


    del df_base
    gc.collect()
    db.excluir_ocorrencias_ano(ano)
    for index, o in ocorrencias_vias_publicas.iterrows():
        ocorrencia = Ocorrencia(
            o["ANO_BO"],
            o["NUM_BO"],
            o["DESCR_TIPOLOCAL"],
            "{}".format(o["LOGRADOURO"]).split(",")[0],
            o["LATITUDE"],
            o["LONGITUDE"],
            o["NATUREZA_APURADA"],
            o['DESC_PERIODO'],
            o["BAIRRO"],
            o["CIDADE"],
            o['DATA_OCORRENCIA_BO'],
            o['NOME_DELEGACIA']
        )

        query_cidade_bairro = {'cidade': normalizar(ocorrencia.cidade),
                               'bairro': normalizar(ocorrencia.bairro)
                               }
        doc_cidade_bairro = db.buscar_documento_unico('ocorrencias-detalhadas', query_cidade_bairro)
        if doc_cidade_bairro:
            query_rua = {'rua': normalizar(ocorrencia.rua)}
            doc_rua = db.buscar_documento_unico('ocorrencias-detalhadas', query_rua)

            ocorrencia.rua = normalizar(ocorrencia.rua)
            ocorrencia.bairro = normalizar(ocorrencia.bairro)
            ocorrencia.cidade = normalizar(ocorrencia.cidade)
            if doc_rua is None:
                ocorrencia = buscar_informacoes_endereco_ocorrencia(ocorrencia)

        else:
            ocorrencia = buscar_informacoes_endereco_ocorrencia(ocorrencia)


        if (ocorrencia.periodo is None or ocorrencia == "Em hora incerta") and o['HORA_OCORRENCIA_BO'] is not None:
            ocorrencia.periodo = definir_periodo(o['HORA_OCORRENCIA_BO'])

        geojson = {'type': "Point", 'coordinates': [float(ocorrencia.lat), float(ocorrencia.lng)]}
        mongo_insert = {
            'localizacao': geojson,
            'crime': ocorrencia.crime,
            'ano': ocorrencia.ano,
            'rua': ocorrencia.rua,
            'bairro': ocorrencia.bairro,
            'delegacia': ocorrencia.delegacia,
            'cidade': ocorrencia.cidade,
            'data_ocorrencia': ocorrencia.data,
            'periodo': ocorrencia.periodo
        }

        try:
            db.inserir_mongo(mongo_insert)
        finally:
            pass

    ocorrencias_vias_publicas.to_csv(f"./backups/dados_tratados_ano_{ano}_{hoje.strftime('%Y_%m_%d')}.csv", sep=';',
                                     encoding='utf-8',
                                     index=False)

    del ocorrencias_vias_publicas
    gc.collect()


def buscar_informacoes_endereco_ocorrencia(ocorrencia):
    endereco_busca = reverter_coordenada_em_endereco(ocorrencia.lat, ocorrencia.lng)
    if endereco_busca is None:
        ocorrencia.rua = ocorrencia.rua.strip()
        ocorrencia.bairro = ocorrencia.bairro.strip()
        ocorrencia.cidade = ocorrencia.cidade.strip()

        if ocorrencia.cidade == "S.PAULO":
            ocorrencia.cidade = "SAO PAULO"

    else:
        ocorrencia.rua = normalizar(endereco_busca['rua']) if endereco_busca[
                                                                  'rua'] != "" else ocorrencia.rua
        ocorrencia.cidade = normalizar(endereco_busca['cidade']) if endereco_busca[
                                                                        'cidade'] != "" else ocorrencia.cidade
        ocorrencia.bairro = normalizar(endereco_busca['bairro']) if endereco_busca[
                                                                        'bairro'] != "" else ocorrencia.bairro

        if ocorrencia.cidade == "S.PAULO":
            ocorrencia.cidade = "SAO PAULO"
    return ocorrencia


def ler_csv(caminho_arquivo, ano):
    Db().excluir_ocorrencias_ano(ano)
    print("Lendo CSV")
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
        geojson = {'type': "Point", 'coordinates': [float(ocorrencia.lat), float(ocorrencia.lng)]}
        mongo_insert = {
            'localizacao': geojson,
            'crime': ocorrencia.crime,
            'ano': ocorrencia.ano,
            'rua': ocorrencia.rua,
            'bairro': ocorrencia.bairro,
            'delegacia': ocorrencia.delegacia,
            'cidade': ocorrencia.cidade,
            'data_ocorrencia': ocorrencia.data,
            'periodo': ocorrencia.periodo
        }
        Db().inserir_mongo(mongo_insert)


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


def normalizar(texto):
    texto = "{}".format(texto)
    return "{}".format(unidecode(texto)).upper()
