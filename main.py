from datetime import datetime

hoje = datetime.now()
from data import ler_base_excel
from models.Base import Base
from models.Db import Db

menu = """
GoGood - Extração de Ocorrências da base do SSP
"""


def main():


    print("Consultando bases atuais...")
    bases: list[Base] = [

        Base(arquivo="./temp/ocorrencias_temp_2024.xlsx", ano_base=2024),
        Base(arquivo="./temp/ocorrencias_temp_2023.xlsx", ano_base=2024),
      ]

    for base in bases:
        print("Cadastrando ocorrências - {}".format(base.ano_base))
        print("Realizando download da base...")
        ler_base_excel(base.arquivo, base.ano_base)


if __name__ == '__main__':
    main()
