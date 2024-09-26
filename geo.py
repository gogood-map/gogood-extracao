from geopy.geocoders import Nominatim
import asyncio
from geopy.adapters import AioHTTPAdapter
import uuid


async def reverter_coordenada_em_endereco(lat, lng):
    agent = "gogood-request-" + str(uuid.uuid4())

    async with Nominatim(user_agent=agent, adapter_factory=AioHTTPAdapter, timeout=500) as nominatim:
        busca = await nominatim.reverse(f"{lat}, {lng}", language="pt-br")

        if busca is None:
            return "", "", ""
        if 'address' in busca.raw:
            rua = busca.raw['address'].get('road', "")
            bairro = busca.raw['address'].get('quarter', "")
            cidade = busca.raw['address'].get('city', "")

            return rua, bairro, cidade
        else:
            return "", "", ""
