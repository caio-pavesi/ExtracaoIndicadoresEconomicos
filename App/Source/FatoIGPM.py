import sys
sys.path.append('App')

import io
import locale
import sqlite3
import requests
import numpy as np
import pandas as pd

from pandas import DataFrame
from datetime import datetime
from bs4 import BeautifulSoup

from Config.folders import ONEDRIVE


def main() -> bool:
    """
    """

    locale.setlocale(locale.LC_ALL, 'pt_BR.UTF-8')
    
    extracted = extract()
    transformed = transform(extracted)
    loaded = load(transformed)

    return True if loaded else False


def extract():

    soup = BeautifulSoup(requests.get('https://www.dadosdemercado.com.br/indices/igp-m').text, "html.parser")

    FatoIGPM = soup.find("table", class_ = "normal-table", id = False)

    FatoIGPM = io.StringIO(str(FatoIGPM).replace(",", "."))    # ! replace comma with dot, UNREPLACEABLE after this step.
    
    return pd.read_html(FatoIGPM)[0]


def transform(FatoIGPM: DataFrame) -> DataFrame:

    FatoIGPM.insert(
        loc = 0
        ,column = "Data Referencia"
        ,value = FatoIGPM["Mês/Ano"].apply(lambda date: datetime.strptime(date.lower(), "%b/%Y"))
    )

    FatoIGPM = FatoIGPM.rename(
        columns = {
            "Índice do mês (%)": "Variacao Mes"
            ,"Acumulado no ano (%)": "Variacao Ano"
            ,"Acumulado 12 meses (%)": "Variacao 12m"
        }
    )

    FatoIGPM = FatoIGPM.replace("--", np.nan)

    FatoIGPM = FatoIGPM.astype(
        dtype = {
            "Variacao Mes": float
            ,"Variacao Ano": float
            ,"Variacao 12m": float
        }
    )

    FatoIGPM = FatoIGPM.drop("Mês/Ano", axis = 1)

    return FatoIGPM[FatoIGPM["Data Referencia"] == datetime(datetime.now().year, datetime.now().month - 2, 1)]


def load(FatoIGPM: DataFrame) -> bool:
    try:
        with sqlite3.connect(ONEDRIVE / "Manchester - Mesa RV - Backoffice - 0/Dados/economia.db") as connection:
            FatoIGPM.to_sql("FatoIGPM", connection, if_exists = "append", index = False)
            return True
    except sqlite3.IntegrityError: return False


if __name__ == "__main__":
    main()