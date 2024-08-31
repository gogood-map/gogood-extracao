from geopy.geocoders import Nominatim


def reverter_coordenada_em_endereco(lat, lng):
    nominatim = Nominatim(user_agent="gogood")
    busca = nominatim.reverse("{}, {}".format(lat, lng), language="pt-br")
    if 'address' in busca:

        rua = busca.raw['address']['road'] if 'road' in busca.raw['address'] else ""
        bairro = busca.raw['address']['quarter'] if 'road' in busca.raw['address'] else ""
        cidade = busca.raw['address']['city'] if 'road' in busca.raw['address'] else ""
        endereco = {
            "rua": rua,
            "cidade": bairro,
            "bairro": cidade,
        }

        return endereco
    else:
        return None
