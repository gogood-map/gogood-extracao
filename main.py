from web import consultar_bases_disponiveis
from data import ler_base
menu = """
GoGood - Wizard de extração
"""



opcoes_database = """
1 - MYSQL
2 - MongoDB
3 - Ambos
"""




def main():
    print(menu)
    print("Consultando bases atuais...")
    print("\nSelecione a base que deseja extrair:")
    bases = consultar_bases_disponiveis()
    for i, b in enumerate(bases):
        print("{} - {}".format(i+1, b.ano_base))

    print("")
    base_escolhida = bases[int(input())-1]
    ler_base(base_escolhida)



if __name__ == '__main__':
    main()
