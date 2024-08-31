import os
from datetime import datetime

hoje = datetime.now()
from data import ler_base_excel
from data import ler_csv
from models.Base import Base
from web import consultar_bases_disponiveis
from web import download
from db import Db
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

        caminho_excel_temp = f"./temp/ocorrencias_temp_{base.ano_base}.xlsx"
        caminho_csv = f"./backups/dados_tratados_ano_{base.ano_base}_{hoje.strftime('%Y_%m_%d')}.csv"

        if os.path.exists(caminho_csv):
            print("CSV encontrado.")
            ler_csv(caminho_csv, base.ano_base)
        else:
            if os.path.exists(caminho_excel_temp):
                print("EXCEL encontrado.")
                ler_base_excel(caminho_excel_temp, base.ano_base)
            else:
                print("Realizando download da base...")
                caminho_arquivo = download(base.arquivo, base.ano_base)
                ler_base_excel(caminho_arquivo, base.ano_base)


if __name__ == '__main__':
    main()
