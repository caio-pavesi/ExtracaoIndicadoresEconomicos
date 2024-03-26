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


def extract():
    return DataFrame(requests.get("https://apisidra.ibge.gov.br/values/t/1737/n1/all/v/all/p/all/d/v63%202,v69%202,v2266%2013,v2263%202,v2264%202,v2265%202?formato=json").json())


def transform(FatoIPCA: DataFrame) -> DataFrame:
    """
    """

    def ipca(FatoIPCA: DataFrame, D2N: str) -> DataFrame:
        """
        """

        def tipo(D2N: str) -> str:
            if D2N == 'IPCA - Variação mensal': return "Variacao Mes"
            if D2N == 'IPCA - Variação acumulada no ano': return "Variacao Ano"
            if D2N == 'IPCA - Variação acumulada em 12 meses': return "Variacao 12m"
            if D2N == 'IPCA - Variação acumulada em 6 meses': return "Variacao 6m"
            if D2N == 'IPCA - Variação acumulada em 3 meses': return "Variacao 3m"

        FatoIPCA = FatoIPCA[(FatoIPCA["MN"] == '%') & (FatoIPCA["D2N"] == D2N)]

        FatoIPCA = FatoIPCA.astype(
            dtype = {
                "V": float
                ,"D3N": str
            }
        )

        FatoIPCA.insert(
            loc = 0
            ,column = "Data Referencia"
            ,value = FatoIPCA["D3N"].apply(lambda date: datetime.strptime(date, "%B %Y"))
        )

        FatoIPCA = FatoIPCA.reindex(
            columns = [
                "Data Referencia"
                ,"V"
            ]
        )

        FatoIPCA = FatoIPCA.rename(
            columns = {
                "V": tipo(D2N)
            }
        )

        FatoIPCA = FatoIPCA[FatoIPCA["Data Referencia"] == datetime(datetime.now().year, datetime.now().month - 2, 1)]

        return FatoIPCA

    FatoIPCA = FatoIPCA.drop(FatoIPCA.index[0]).reset_index(drop=True)

    FatoIpca1m = ipca(FatoIPCA, 'IPCA - Variação mensal')
    FatoIpca1a = ipca(FatoIPCA, 'IPCA - Variação acumulada no ano')
    FatoIpca12m = ipca(FatoIPCA, 'IPCA - Variação acumulada em 12 meses')
    FatoIpca6m = ipca(FatoIPCA, 'IPCA - Variação acumulada em 6 meses')
    FatoIpca3m = ipca(FatoIPCA, 'IPCA - Variação acumulada em 3 meses')

    FatoIPCA = FatoIpca1m.merge(
        FatoIpca1a, on = "Data Referencia", how = "inner"
    ).merge(
        FatoIpca12m, on = "Data Referencia", how = "inner"
    ).merge(
        FatoIpca6m, on = "Data Referencia", how = "inner"
    ).merge(
        FatoIpca3m, on = "Data Referencia", how = "inner"
    )

    return FatoIPCA


def load(FatoIPCA: DataFrame) -> bool:
    try:
        with sqlite3.connect(ONEDRIVE / "Manchester - Mesa RV - Backoffice - 0/Dados/economia.db") as connection:
            FatoIPCA.to_sql("FatoIPCA", connection, if_exists = "append", index = False)
            return True
    except sqlite3.IntegrityError: return False # ! data already exists.


if __name__ == "__main__":
    main()