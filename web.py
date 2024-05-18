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


def obter_rua_coordenada(lat, lng):
    lat_str = str(lat)
    lng_str = str(lng)
    uri = "https://nominatim.openstreetmap.org/reverse.php?lat={}&lon={}&zoom=18&format=jsonv2".format(
        lat_str.replace(",", "."), lng_str.replace(",", "."))

    request = requests.get(uri)
    json_resposta = json.loads(request.text)
    try:
        rua = json_resposta['address']['road']
    except:
        rua = ""
    return rua

