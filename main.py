import datetime
import os
from datetime import datetime

from data import ler_base_excel
from data import ler_csv
from db import cadastrar
from models.Banco import Banco
from models.Base import Base
from web import consultar_bases_disponiveis
from web import download

menu = """
GoGood - Wizard de extração
"""
opcoes_database = """
Escolha qual base de dados vai receber a extração
1 - MYSQL
2 - MongoDB
3 - MYSQL / MongoDB
"""

bancos = [Banco.MYSQL, Banco.MONGO, None]


def main():
    print(menu)
    print(opcoes_database)
    banco_escolhido = bancos[int(input()) - 1]
    hoje = datetime.datetime.now()
    print("Consultando bases atuais...")
    print("\nSelecione a base que deseja extrair:")
    bases: list[Base] = consultar_bases_disponiveis()

    print("{} - {}/2".format(1, bases[0].ano_base))
    print("{} - {}/1 | {}/2".format(2, bases[1].ano_base, bases[1].ano_base))

    base_escolhida = bases[int(input()) - 1]

    caminho_excel_temp = f"./temp/ocorrencias_temp_{base_escolhida.ano_base}.xlsx"
    caminho_csv = f"./backups/{base_escolhida}_{hoje.strftime('%Y_%m_%d')}.csv"

    if os.path.exists(caminho_csv):

        ocorrencias = ler_csv(caminho_csv)
    else:
        if os.path.exists(caminho_excel_temp):
            ocorrencias = ler_base_excel(caminho_excel_temp, base_escolhida.ano_base)
        else:
            caminho_arquivo = download(base_escolhida.arquivo, base_escolhida.ano_base)
            ocorrencias = ler_base_excel(caminho_arquivo, base_escolhida.ano_base)

    cadastrar(banco_escolhido, ocorrencias, base_escolhida)


if __name__ == '__main__':
    main()
