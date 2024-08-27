from typing import TextIO


def criar(caminho):
    file = open(caminho, "a")
    return file


def escrever(arquivo: TextIO, conteudo):
    arquivo.write(conteudo)
