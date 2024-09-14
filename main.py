import os
from datetime import datetime

hoje = datetime.now()
from data import ler_base_excel
from models.Base import Base
from web import consultar_bases_disponiveis
from models.Db import Db
menu = """
GoGood - Extração de Ocorrências da base do SSP
"""


def main():
    db = Db()
    db.conectar_mongodb()
    print("Consultando bases atuais...")
    bases: list[Base] = consultar_bases_disponiveis()

    for base in bases:
        print("Cadastrando ocorrências - {}".format(base.ano_base))
        print("Realizando download da base...")
        ler_base_excel(base.arquivo, base.ano_base)


if __name__ == '__main__':
    main()
