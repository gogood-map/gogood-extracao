from decimal import Decimal, getcontext

import pandas
from models.Base import Base

colunas_filtro = ["E", "F", "K", "O", "P", "W"]
labels: list[str] = ["ANO_BO", "NUM_BO", "DESCR_TIPOLOCAL", "RUA", "LATITUDE", "LONGITUDE", "CRIME"]



def ler_base(base: Base):
    planilhas = []
    if base.ano_base != '2024':
        planilhas = [0, 1]
    else:
        planilhas = [0]

    df_base = pandas.read_excel("https://www.ssp.sp.gov.br/" + base.arquivo, sheet_name=planilhas,
                                usecols="E,F,K,M,O,P,W",
                                dtype={'LATITUDE': str, 'LONGITUDE': str})

    if base.ano_base != '2024':
        df_base = pandas.concat([df_base[0], df_base[1]])
    else:
        df_base = df_base[0]

    df_base.columns = labels
    coordenadas_invalidas = df_base.query('LATITUDE.isnull() | LONGITUDE.isnull() | LATITUDE == 0 | LONGITUDE == 0',
                                          engine='python').index
    df_base.drop(coordenadas_invalidas, axis=0, inplace=True)
    ocorrencias_vias_publicas = df_base.query(
        'DESCR_TIPOLOCAL == "Via Pública" | DESCR_TIPOLOCAL == "Ciclofaixa" | DESCR_TIPOLOCAL == "Praça"')

    return ocorrencias_vias_publicas

