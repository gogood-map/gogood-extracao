import json
from datetime import datetime

import requests
import os
from models.Base import Base


# def consultar_bases_disponiveis():
#     ano_atual = datetime.now().year
#     req = requests.get("http://www.ssp.sp.gov.br/assets/estatistica/transparencia/spDados160_516.json").text
#     json_res = json.loads(req)
#     bases = json_res['data'][0]['lista']
#     list_bases = []
#
#     for b in bases:
#         if ano_atual - int(b['periodo']) < 2:
#             list_bases.append(Base(b['periodo'], "http://www.ssp.sp.gov.br/" + b['arquivo']))
#
#     return list_bases



