from db import conectar_mysql

def test_conexao_banco():
    cursor = conectar_mysql()
    cursor.executemany()