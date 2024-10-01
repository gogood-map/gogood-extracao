from datetime import datetime

import pandas
from pandas import DataFrame

from models.Enderecos import Enderecos

hoje = datetime.now()
from data import ler_base
from data import pre_insercao_ocorrencias
from models.Base import Base
import asyncio
import glob

menu = """
GoGood - Extração de Ocorrências da base do SSP
"""

enderecos = Enderecos()


async def main():
    print("Consultando bases atuais...")

    base: Base = Base(arquivo="./temp/ocorrencias_temp_2024.csv", ano_base=2024)

    print("Cadastrando ocorrências - {}".format(base.ano_base))
    print("Realizando leitura da base...")
    try:
        await ler_base(base.arquivo, base.ano_base, enderecos)
    except BaseException or asyncio.exceptions.CancelledError or KeyboardInterrupt or Exception as e:
        from data import df


        print(e)
        pickle_anterior = glob.glob("./backup/*.pkl")

        import os
        if len(pickle_atual) > 0:
            os.remove(pickle_anterior[0])

        df.to_pickle("./backup/df_insercao_{}_{}_{}.pkl".format(
            datetime.now().year, datetime.now().month, datetime.now().day
        ))


if __name__ == '__main__':

    pickle_atual = glob.glob("./backup/*.pkl")
    if len(pickle_atual) > 0:
        from models.Db import Db



        df_backup: DataFrame = pandas.read_pickle(pickle_atual[0])
        print("Continuando inserção... \nArquivo: {}".format(pickle_atual[0]))
        asyncio.run(pre_insercao_ocorrencias(df_backup, enderecos))

    asyncio.run(main())
