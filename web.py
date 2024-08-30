import json
import os
from datetime import datetime

import requests

from models.Base import Base


def consultar_bases_disponiveis():
    ano_atual = datetime.now().year
    req = requests.get("http://www.ssp.sp.gov.br/assets/estatistica/transparencia/spDados160_516.json").text
    json_res = json.loads(req)
    bases = json_res['data'][0]['lista']
    list_bases = []

    for b in bases:
        if ano_atual - int(b['periodo']) < 2:
            list_bases.append(Base(b['periodo'], "http://www.ssp.sp.gov.br/" + b['arquivo']))

    return list_bases


def download(url, ano):
    print("Realizando download do arquivo...")
    from requests.adapters import HTTPAdapter
    from urllib3.util.retry import Retry

    retry_strategy = Retry(
        total=3,
        backoff_factor=1
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    http = requests.Session()
    http.mount("https://", adapter)
    http.mount("http://", adapter)

    req = http.get(url, stream=True)
    caminho_arquivo = './temp/ocorrencias_temp_{}.xlsx'.format(ano)

    if os.path.exists(caminho_arquivo):
        os.remove(caminho_arquivo)

    with open(caminho_arquivo, 'wb') as f:
        for chunk in req.iter_content(chunk_size=16 * 1024):
            f.write(chunk)

    return caminho_arquivo

