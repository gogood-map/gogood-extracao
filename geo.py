from geopy.geocoders import Nominatim
import json

def reverter_coordenada_em_endereco(lat, lng):
    nominatim = Nominatim(user_agent="gogood")
    busca = nominatim.reverse("{}, {}".format(lat, lng), language="pt-br")
    if hasattr(busca, 'address'):
        endereco = {
            "rua": busca.raw['address']['road'],
            "cidade": busca.raw['address']['city'],
            "bairro": busca.raw['address']['quarter'],
        }
        return endereco
    else:
        return None
