class Ocorrencia:
    ano: int
    num_bo: str
    local: str
    rua: str
    lat: str
    lng: str
    crime: str
    periodo: str
    bairro: str
    cidade: str
    data: str
    delegacia: str

    def __init__(self, ano, num_bo, local, rua, lat, lng, crime, periodo, bairro, cidade, data, delegacia):
        self.data = data
        self.cidade = cidade
        self.bairro = bairro
        self.crime = crime
        self.lng = lng[:12] if len(lng) > 13 else lng
        self.lat = lat[:12] if len(lat) > 13 else lat
        self.rua = rua
        self.local = local
        self.num_bo = num_bo
        self.ano = ano
        self.periodo = periodo
        self.delegacia = delegacia
        self.tratar_coordenada()

    def tratar_coordenada(self):
        latitude: str = self.lat
        longitude: str = self.lng

        if "." not in latitude:
            lat_formatada = ""
            i = 0
            for caractere in latitude:
                lat_formatada += "{}".format(caractere)
                i += 1
                if i == 3:
                    lat_formatada += "."
            self.lat = lat_formatada
        if "." not in longitude:
            lng_formatada = ""
            i = 0
            for caractere in longitude:
                lng_formatada += "{}".format(caractere)
                i += 1
                if i == 3:
                    lng_formatada += "."
            self.lng = lng_formatada
