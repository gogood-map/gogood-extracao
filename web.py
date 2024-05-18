import requests
import json
from models.Base import Base
def consultar_bases_disponiveis():
    req = requests.get("https://www.ssp.sp.gov.br/assets/estatistica/transparencia/spDados160_516.json").text
    json_res =json.loads(req)
    bases = json_res['data'][0]['lista']
    list_bases = []

    for b in bases:
        list_bases.append(Base(b['periodo'], b['arquivo']))

    return list_bases

