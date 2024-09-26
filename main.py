from datetime import datetime

from models.Enderecos import Enderecos

hoje = datetime.now()
from data import ler_base_excel
from models.Base import Base
import asyncio

menu = """
GoGood - Extração de Ocorrências da base do SSP
"""



async def main():
    print("Consultando bases atuais...")

    bases: list[Base] = [
        Base(arquivo="./temp/ocorrencias_temp_2024.CSV", ano_base=2024),
        Base(arquivo="./temp/ocorrencias_temp_2023.CSV", ano_base=2023),
    ]
    enderecos = Enderecos()
    for base in bases:
        print("Cadastrando ocorrências - {}".format(base.ano_base))
        print("Realizando leitura da base...")
        await ler_base_excel(base.arquivo, base.ano_base, enderecos)


if __name__ == '__main__':
    asyncio.run(main())
