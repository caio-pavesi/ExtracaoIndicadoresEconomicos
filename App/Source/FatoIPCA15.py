import sys
sys.path.append('App')

import locale
import sqlite3
import requests

from pandas import DataFrame
from datetime import datetime

from Config.folders import ONEDRIVE


def main() -> bool:
    """
    """

    locale.setlocale(locale.LC_ALL, 'pt_BR.UTF-8')
    
    extracted = extract()
    transformed = transform(extracted)
    loaded = load(transformed)

    return True if loaded else False


def extract() -> DataFrame:
    return DataFrame(requests.get("https://apisidra.ibge.gov.br/values/t/7062/n1/all/v/all/p/all/c315/7169/d/v355%202,v356%202,v357%204,v1120%202?formato=json").json())


def transform(FatoIPCA15: DataFrame) -> DataFrame:
    """
    """

    def ipca(FatoIPCA15: DataFrame, D2N: str) -> DataFrame:
        """
        """

        def tipo(D2N: str) -> str:
            if D2N == 'IPCA15 - Variação mensal': return "Variacao Mes"
            if D2N == 'IPCA15 - Variação acumulada no ano': return "Variacao Ano"
            if D2N == 'IPCA15 - Variação acumulada em 12 meses': return "Variacao 12m"

        FatoIPCA15 = FatoIPCA15[(FatoIPCA15["MN"] == '%') & (FatoIPCA15["D2N"] == D2N)]

        FatoIPCA15 = FatoIPCA15.astype(
            dtype = {
                "V": float
                ,"D3N": str
            }
        )

        FatoIPCA15.insert(
            loc = 0
            ,column = "Data Referencia"
            ,value = FatoIPCA15["D3N"].apply(lambda date: datetime.strptime(date, "%B %Y"))
        )

        FatoIPCA15 = FatoIPCA15.reindex(
            columns = [
                "Data Referencia"
                ,"V"
            ]
        )

        FatoIPCA15 = FatoIPCA15.rename(
            columns = {
                "V": tipo(D2N)
            }
        )

        FatoIPCA15 = FatoIPCA15[FatoIPCA15["Data Referencia"] == datetime(datetime.now().year, datetime.now().month - 2, 1)]

        return FatoIPCA15

    FatoIPCA15 = FatoIPCA15.drop(FatoIPCA15.index[0]).reset_index(drop=True)

    FatoIpca151m = ipca(FatoIPCA15, 'IPCA15 - Variação mensal')
    FatoIpca151a = ipca(FatoIPCA15, 'IPCA15 - Variação acumulada no ano')
    FatoIpca1512m = ipca(FatoIPCA15, 'IPCA15 - Variação acumulada em 12 meses')

    FatoIPCA15 = FatoIpca151m.merge(
        FatoIpca151a, on = "Data Referencia", how = "inner"
    ).merge(
        FatoIpca1512m, on = "Data Referencia", how = "inner"
    )

    return FatoIPCA15


def load(FatoIPCA15: DataFrame) -> bool:
    try:
        with sqlite3.connect(ONEDRIVE / "Manchester - Mesa RV - Backoffice - 0/Dados/economia.db") as connection:
            FatoIPCA15.to_sql("FatoIPCA15", connection, if_exists = "append", index = False)
            return True
    except sqlite3.IntegrityError: return False # ! data already exists.


if __name__ == "__main__":
    main()