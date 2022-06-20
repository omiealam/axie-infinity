from urllib.request import urlopen
from io import BytesIO
from zipfile import ZipFile

currencies = {"BIDR", "BNB", "BUSD", "ETH", "TRY", "USDT"}  # Second element of the SLP trading pair, e.g. currencies that SLP can be traded for on Binance

trading_months = {  # Months with SLP-(destination currency) trading data on Binance
    "BIDR" : {"2021-12", "2022-01", "2022-02", "2022-03"},
    "BNB"  : {"2022-02", "2022-03"},
    "BUSD" : {"2021-04", "2021-05", "2021-06", "2021-07", "2021-08", "2021-09", "2021-10", "2021-11", "2021-12", "2022-01", "2022-02", "2022-03"},
    "ETH"  : {"2020-11", "2020-12", "2021-01", "2021-02", "2021-03", "2021-04", "2021-05", "2021-06", "2021-07", "2021-08", "2021-09", "2021-10", "2021-11", "2021-12", "2022-01", "2022-02", "2022-03"},
    "TRY"  : {"2021-11", "2021-12", "2022-01", "2022-02", "2022-03"},
    "USDT" : {"2021-04", "2021-05", "2021-06", "2021-07", "2021-08", "2021-09", "2021-10", "2021-11", "2021-12", "2022-01", "2022-02", "2022-03"}
}


for currency in currencies: # Iterate over destination currencies
  for month in trading_months[currency]:  # Iterate over relevant months
    trading_pair = "SLP" + currency
    curr_path = '/data/binance-data/slp-trading-data/' + currency + "/" 
    url = "https://data.binance.vision/data/spot/monthly/trades/" + trading_pair + "/" + trading_pair + "-trades-" + month + ".zip" # Assemble URL with relevant ZIP
    http_response = urlopen(url)
    zipfile = ZipFile(BytesIO(http_response.read()))
    zipfile.extractall(path=curr_path)
