import sys
sys.path.append('C:/Automations/5/App')

import io
import re
import sqlite3
import requests
import numpy as np
import pandas as pd

from pdfminer.pdfparser import PDFSyntaxError 
from datetime import datetime, timedelta
from pandas import DataFrame, DateOffset

from Modules.pdf import Pdf
from Config.folders import ONEDRIVE


def main(date) -> bool:
    """
    """
    try:
        friday = get_pub_date(date)
        extracted = extract(friday)
        transformed = transform(extracted, friday)
        loaded = load(transformed)
    except PDFSyntaxError:
        print("Documento não encontrado.")

    return True if loaded else False


def get_pub_date(date: datetime) -> datetime:
    """Returns the date of the previous friday."""
    offset = 0
    while (get_pub_date := date - timedelta(days = offset)).weekday() != 4:
        offset += 1

    return datetime(get_pub_date.year, get_pub_date.month, get_pub_date.day)


def sum_year(current_date: datetime, years: int) -> datetime:
    return datetime.fromisoformat((current_date + DateOffset(years = years)).isoformat())


def extract(date: datetime) -> io.BytesIO:
    print(f"https://www.bcb.gov.br/content/focus/focus/R{get_pub_date(date).strftime("%Y%m%d")}.pdf")
    return io.BytesIO(requests.get(f"https://www.bcb.gov.br/content/focus/focus/R{get_pub_date(date).strftime("%Y%m%d")}.pdf").content)


def transform(file: io.BytesIO, date: datetime) -> DataFrame:

    column1 = list()
    column2 = list()
    column3 = list()
    column4 = list()
    
    for line in Pdf(file).read_text().split("\n"):
        # Breakthrough: https://regex101.com/r/O4EOaf/1
        if (matches := re.match(r"^(IPCA \(variação %\)|PIB Total \(variação % sobre ano anterior\)|Câmbio \(R\$\/US\$\)|Selic \(% a\.a\)|IGP-M \(variação %\)|IPCA Administrados \(variação %\)|Conta corrente \(US\$ bilhões\)|Balança comercial \(US\$ bilhões\)|Investimento direto no país \(US\$ bilhões\)|Dívida líquida do setor público \(% do PIB\)|Resultado primário \(% do PIB\)|Resultado nominal \(% do PIB\)) (([-\d,]+|-) ([-\d,]+|-) ([-\d,]+|-)( \W \(\d\)| \(\d\))?( [\d]+)?( [-\d,]+|-)?( [\d]+)?){1} (([-\d,]+|-) ([-\d,]+|-) ([-\d,]+|-)( \W \(\d\)| \(\d\))?( [\d]+)?( [-\d,]+|-)?( [\d]+)?){1} (([-\d,]+|-) ([-\d,]+|-) ([-\d,]+|-)( \W \(\d+\)| \(\d+\))?( [\d]+)?){1} (([-\d,]+|-) ([-\d,]+|-) ([-\d,]+|-)( \W \(\d+\)| \(\d+\))?( [\d]+)?){1}$", line)):

            column1.append({
                "Data Referencia": date
                ,"Ano Referencia": date
                ,"Indicador": matches.group(1) 
                ,"4 Semanas": matches.group(3) if matches.group(3) != "-" else (np.nan)
                ,"1 Semana": matches.group(4) if matches.group(4) != "-" else (np.nan)
                ,"Hoje": matches.group(5) if matches.group(5) != "-" else (np.nan)
                ,"Respondentes 30 Dias": matches.group(7) if matches.group(7) else (np.nan)
                ,"5 Dias Uteis": matches.group(8) if matches.group(8) else (np.nan)
                ,"Respondentes 5 Dias": matches.group(9) if matches.group(9) else (np.nan)
            })

            column2.append({
                "Data Referencia": date
                ,"Ano Referencia": sum_year(get_pub_date(date), 1)
                ,"Indicador": matches.group(1) 
                ,"4 Semanas": matches.group(11) if matches.group(11) != "-" else (np.nan)
                ,"1 Semana": matches.group(12) if matches.group(12) != "-" else (np.nan)
                ,"Hoje": matches.group(13) if matches.group(13) != "-" else (np.nan)
                ,"Respondentes 30 Dias": matches.group(15) if matches.group(15) else (np.nan)
                ,"5 Dias Uteis": matches.group(16) if matches.group(16) else (np.nan)
                ,"Respondentes 5 Dias": matches.group(17) if matches.group(17) else (np.nan)
            })

            column3.append({
                "Data Referencia": date
                ,"Ano Referencia": sum_year(get_pub_date(date), 2)
                ,"Indicador": matches.group(1)
                ,"4 Semanas": matches.group(19) if matches.group(19) != "-" else (np.nan)
                ,"1 Semana": matches.group(20) if matches.group(20) != "-" else (np.nan)
                ,"Hoje": matches.group(21) if matches.group(21) != "-" else (np.nan)
                ,"Respondentes 30 Dias": matches.group(23) if matches.group(23) != "-" else (np.nan)
            })

            column4.append({
                "Data Referencia": date
                ,"Ano Referencia": sum_year(get_pub_date(date), 3)
                ,"Indicador": matches.group(1)
                ,"4 Semanas": matches.group(25) if matches.group(25) != "-" else (np.nan)
                ,"1 Semana": matches.group(26) if matches.group(26) != "-" else (np.nan)
                ,"Hoje": matches.group(27) if matches.group(27) != "-" else (np.nan)
                ,"Respondentes 30 Dias": matches.group(29) if matches.group(29) != "-" else (np.nan)
            })

    column1 = DataFrame(column1)
    column2 = DataFrame(column2)
    column3 = DataFrame(column3)
    column4 = DataFrame(column4)

    FatoExpectativasBCB: DataFrame = pd.concat([column1, column2, column3, column4], ignore_index = True)

    FatoExpectativasBCB[["4 Semanas", "1 Semana", "Hoje", "5 Dias Uteis"]] = FatoExpectativasBCB[["4 Semanas", "1 Semana", "Hoje", "5 Dias Uteis"]].map(lambda number: str(number).replace(",", "."))

    FatoExpectativasBCB = FatoExpectativasBCB.astype(
        dtype = {
            "Data Referencia": "datetime64[ns]"
            ,"Ano Referencia": "datetime64[ns]"
            ,"Indicador": str
            ,"4 Semanas": "float64"
            ,"1 Semana": "float64"
            ,"Hoje": "float64"
            ,"Respondentes 30 Dias": "float64"
            ,"5 Dias Uteis": "float64"
            ,"Respondentes 5 Dias": "float64"
        } 
    )

    return FatoExpectativasBCB


def load(FatoExpectativasBCB: DataFrame) -> bool:
    try:
        database = pd.read_sql("SELECT * FROM FatoExpectativasBCB", sqlite3.connect(ONEDRIVE / "Manchester - Mesa RV - Backoffice - 0/Dados/economia.db")).astype(dtype = {"Data Referencia": "datetime64[ns]"})
        with sqlite3.connect(ONEDRIVE / "Manchester - Mesa RV - Backoffice - 0/Dados/economia.db") as connection:
            FatoExpectativasBCB[~FatoExpectativasBCB["Data Referencia"].isin(database["Data Referencia"])].to_sql("FatoExpectativasBCB", connection, if_exists = "append", index = False)
            return True
    except Exception as error:
        return False


if __name__ == "__main__":
    main(datetime.now())