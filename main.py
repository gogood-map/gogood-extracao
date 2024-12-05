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

    base: Base = Base(arquivo="./files/SPDadosCriminais_2024.CSV", ano_base=2024)

    print("Cadastrando ocorrências - {}".format(base.ano_base))
    print("Realizando leitura da base...")

    await ler_base(base.arquivo, base.ano_base, enderecos)


if __name__ == '__main__':
    asyncio.run(main())
