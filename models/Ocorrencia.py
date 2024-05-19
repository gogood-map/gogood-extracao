class Ocorrencia:
    def __init__(self, ano, num_bo, local, rua, lat, lng, crime):
        self.crime = crime
        self.lng = lng[:12] if len(lng) > 13 else lng
        self.lat = lat[:12] if len(lat) > 13 else lat
        self.rua = rua
        self.local = local
        self.num_bo = num_bo
        self.ano = ano
        self.tratar_coordenada()
    def tratar_coordenada(self):
        latitude: str = self.lat
        longitude: str = self.lng

        if "." not in latitude:
            lat_formatada = ""
            i = 0
            for lat in latitude:
                lat_formatada += "{}".format(lat)
                i += 1
                if i == 3:
                    lat_formatada += "."
            self.lat = lat_formatada
        if "." not in longitude:
            lng_formatada = ""
            i = 0
            for long in longitude:
                lng_formatada += "{}".format(long)
                i += 1
                if i == 3:
                    lng_formatada += "."
            self.lng = lng_formatada






