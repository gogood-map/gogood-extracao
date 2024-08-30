import openpyxl
from openpyxl.workbook import Workbook
from openpyxl.worksheet.worksheet import Worksheet
import openpyxl.utils


class Excel:
    arquivo: Workbook
    aba: Worksheet

    def __init__(self, caminho_arquivo: str):
        self.arquivo = openpyxl.load_workbook(caminho_arquivo)

    def listar_abas(self):
        return self.arquivo.sheetnames

    def remover_coluna_pelo_conteudo_da_celula(self, lista_conteudo, indice_linha):

        indices = []
        i = 0
        for linha in self.aba[indice_linha]:
            if linha.value in lista_conteudo:
                indices.append(i)
            i += 1
        for i in indices:
            self.aba.delete_cols(i)

    def selecionar_aba(self, nome_aba):
        self.aba = self.arquivo[nome_aba]
