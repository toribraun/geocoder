import re
import requests
import zipfile
from preprocessing.parser import interactive_loading

URL_FIAS = r'https://fias.nalog.ru/Updates'
URL_ARCH = r'https://fias-file.nalog.ru/ExportDownloads?file='
ZIP_FIAS = 'fias_xml.zip'


@interactive_loading
def load_xml():
    page = requests.get(URL_FIAS).text
    regex_arch = re.compile(r"(?<=direct_download file_count_link_full' "
                            r"href=').*?(?='>fias_xml.zip )")
    url = regex_arch.search(page).group(0)
    response = requests.get(url, stream=True, timeout=200000)
    with open(ZIP_FIAS, 'wb') as arch:
        i = 0
        for chunk in response.iter_content(chunk_size=1000 ** 2 * 10):
            print(i)
            i += 1
            arch.write(chunk)
    extract_zip()


def extract_zip(zip_name=ZIP_FIAS):
    with zipfile.ZipFile(zip_name, 'r') as zip_file:
        regex_table = re.compile(r'AS_(ADDROBJ|HOUSE)')
        for file in zip_file.namelist():
            if regex_table.match(file):
                zip_file.extract(file)
