import requests
from urllib.parse import urlencode
from preprocessing.parser import interactive_loading

BASE_URL = 'https://cloud-api.yandex.net/v1/disk/public/resources/download?'
URL_DB = 'https://yadi.sk/d/p775DvsYAfVf_A'


@interactive_loading
def load_database_from_disk(database, url=URL_DB):
    response = requests.get(BASE_URL + urlencode(dict(public_key=url)))
    with open(database, 'wb') as db:
        download_response = requests.get(response.json()['href'], stream=True,
                                         timeout=200000)
        for chunk in download_response.iter_content(chunk_size=1000 ** 2 * 10):
            db.write(chunk)
