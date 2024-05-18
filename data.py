import pandas
from models.Base import Base


def ler_base(base: Base):
    planilhas = []
    if base.ano_base != 2024:
        planilhas = [0, 1]
    else:
        planilhas = [0]

    df_base = pandas.read_excel("https://www.ssp.sp.gov.br/" + base.arquivo, sheet_name=planilhas)
    if base.ano_base != 2024:
        df_base = pandas.concat([df_base[0], df_base[1]])
    print(df_base)
