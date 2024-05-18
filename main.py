from web import consultar_bases_disponiveis
from data import ler_base
from db import cadastrar


menu = """
GoGood - Wizard de extração
"""
opcoes_database = """
Escolha qual base de dados vai receber a extração
1 - MYSQL
2 - MongoDB
3 - Ambos
"""


bancos = ["MYSQL", "MONGODB", "AMBOS"]

def main():
    print(menu)
    print("Consultando bases atuais...")
    print("\nSelecione a base que deseja extrair:")
    bases = consultar_bases_disponiveis()
    for i, b in enumerate(bases):
        print("{} - {}".format(i+1, b.ano_base))

    print("")
    base_escolhida = bases[int(input())-1]
    ocorrencias = ler_base(base_escolhida)


    print(opcoes_database)
    banco_escolhido = bancos[int(input())-1]
    cadastrar(banco_escolhido, ocorrencias, base_escolhida)














if __name__ == '__main__':
    main()
