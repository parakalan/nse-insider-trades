from datetime import datetime, timedelta, date
from tqdm.auto import tqdm
import calendar
import json
import pandas as pd
import requests


category_of_person = [
    "Employees/Designated Employees",
    "Immediate relative",
    "Director",
    "Key Managerial Personnel"
]



def get_historic_insider_data(symbol):
    with open(f"nse_insider_scrape/{symbol}.json") as f:
        js = json.load(f)
    return pd.DataFrame(js['data'])


def add_months(sourcedate, months):
    month = sourcedate.month - 1 + months
    year = sourcedate.year + month // 12
    month = month % 12 + 1
    day = min(sourcedate.day, calendar.monthrange(year,month)[1])
    return date(year, month, day)

def get_symbol_price_on_day(symbol, date, price_history=None):
    if price_history is None:
        price_history = pd.read_csv(f"historic_data/{symbol}.csv")
    if date.weekday() == 5:
        date = date + timedelta(2)
    elif date.weekday() == 6:
        date = date + timedelta(1)
    count = 0
    while date.strftime("%Y-%m-%d") not in price_history.Date.tolist():
        date = date + timedelta(1)
        count += 1
        if count >= 5:
            raise RuntimeError(f"{symbol}")
    return price_history[price_history.Date == date.strftime("%Y-%m-%d")]["Open"].tolist()[0]

companies = pd.read_excel("MCAP31032021_0.xlsx")
companies = companies[~companies["Market capitalization \n(Rs in Lakhs)"].isna()]
companies = companies[~companies["Market capitalization \n(Rs in Lakhs)"].isin(["* Not Traded as on March 31, 2021"])]
companies_filtered = companies[companies["Market capitalization \n(Rs in Lakhs)"] > (5000 * 100)]


def process_symbol(symbol):
    df = get_historic_insider_data(symbol)
    df = df[df.acqMode == "Market Purchase"]
    df = df[df.tdpTransactionType == "Buy"]
    df = df[df["personCategory"].isin(category_of_person)]
    df[df.date.apply(lambda m: datetime.strptime(m, "%d-%b-%Y %H:%M").date() >= date(2016, 9, 29))]
    df["Trade Buy Date"] = df.date.apply(lambda m: datetime.strptime(m, "%d-%b-%Y %H:%M").date() + timedelta(days=1))
    df["1 month"] = df["Trade Buy Date"].apply(lambda m: add_months(m, 1))
    df["3 month"] = df["Trade Buy Date"].apply(lambda m: add_months(m, 3))
    df["6 month"] = df["Trade Buy Date"].apply(lambda m: add_months(m, 6))
    df["12 month"] = df["Trade Buy Date"].apply(lambda m: add_months(m, 12))
    price_history = pd.read_csv(f"historic_data/{symbol}.csv")
    df["Buy Price"] = df["Trade Buy Date"].apply(lambda m: get_symbol_price_on_day(symbol, m, price_history))
    df["1 month Price"] = df["1 month"].apply(lambda m: get_symbol_price_on_day(symbol, m, price_history))
    df["3 month Price"] = df["3 month"].apply(lambda m: get_symbol_price_on_day(symbol, m, price_history))
    df["6 month Price"] = df["6 month"].apply(lambda m: get_symbol_price_on_day(symbol, m, price_history))
    df["12 month Price"] = df["12 month"].apply(lambda m: get_symbol_price_on_day(symbol, m, price_history))
    df["1 month Return"] = df.apply(lambda row: 100 * (row["1 month Price"] - row["Buy Price"]) / row["Buy Price"], axis=1)
    df["3 month Return"] = df.apply(lambda row: 100 * (row["3 month Price"] - row["Buy Price"]) / row["Buy Price"], axis=1)
    df["6 month Return"] = df.apply(lambda row: 100 * (row["6 month Price"] - row["Buy Price"]) / row["Buy Price"], axis=1)
    df["12 month Return"] = df.apply(lambda row: 100 * (row["12 month Price"] - row["Buy Price"]) / row["Buy Price"], axis=1)
    df.to_csv(f"insider_analysed/{symbol}.csv")
    print(symbol)
    print("Average 1 month return", df["1 month Return"].mean())
    print("Average 3 month return", df["3 month Return"].mean())
    print("Average 6 month return", df["6 month Return"].mean())
    print("Average 12 month return", df["12 month Return"].mean())
    return (symbol, df["1 month Return"].mean(), df["3 month Return"].mean(), df["6 month Return"].mean(), df["12 month Return"].mean())


symbols = companies_filtered.Symbol.tolist()
for symbol in tqdm(symbols):
    results = process_symbol(symbol)
    
returns_df = pd.DataFrame()
returns_df["SYMBOL"] = [i[0] for i in results]
returns_df["Avg 1 Month Return"] = [i[1] for i in results]
returns_df["Avg 3 Month Return"] = [i[2] for i in results]
returns_df["Avg 6 Month Return"] = [i[3] for i in results]
returns_df["Avg 12 Month Return"] = [i[4] for i in results]
returns_df.to_excel("insider_analysed_final.xlsx")
