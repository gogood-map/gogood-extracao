import os
from datetime import datetime

from data import ler_base_excel
from data import ler_csv
from db import cadastrar
from models.Base import Base
from web import consultar_bases_disponiveis
from web import download

menu = """
GoGood - Wizard de extração
"""


def main():
    hoje = datetime.now()
    print("Consultando bases atuais...")
    print("\nSelecione a base que deseja extrair:")
    bases: list[Base] = consultar_bases_disponiveis()

    print("Cadastrando as bases de: ".format())
    print("{}/2 - {}".format(bases[0].ano_base, bases[1].ano_base))

    for base in bases:

        caminho_excel_temp = f"./temp/ocorrencias_temp_{base.ano_base}.xlsx"
        caminho_csv = f"./backups/dados_tratados_ano_{base.ano_base}_{hoje.strftime('%Y_%m_%d')}.csv"

        if os.path.exists(caminho_csv):
            ocorrencias = ler_csv(caminho_csv)
        else:
            if os.path.exists(caminho_excel_temp):
                ocorrencias = ler_base_excel(caminho_excel_temp, base.ano_base)
            else:
                caminho_arquivo = download(base.arquivo, base.ano_base)
                ocorrencias = ler_base_excel(caminho_arquivo, base.ano_base)

        cadastrar(ocorrencias, base)


if __name__ == '__main__':
    main()
