import snowflake.connector
from sqlalchemy import create_engine
from snowflake.sqlalchemy import URL
from requests import Request, Session
from pandas_datareader.data import DataReader

"""Script to Initialise snowflake database from 1st October 2021 to 1st April 2022"""

engine = create_engine(URL(
    user='weilin',
    password='P@ssword1',
    account='oq82740',
    warehouse="COMPUTE_WH",
    database="PORTFOLIO_REBALANCING",
    schema="STI_DAILY_RAW_DATA",
    region="ap-southeast-1"
))
connection = engine.connect()

tickers = ['U11.SI', 'D05.SI', 'C52.SI',
'BN4.SI',
'V03.SI',
'C38U.SI',
'A17U.SI',
'Z74.SI',
'O39.SI',
'N2IU.SI',
'Y92.SI',
'F34.SI',
'ME8U.SI',
'AJBU.SI',
'C09.SI',
'M44U.SI',
'U96.SI',
'9CI.SI',
'BS6.SI',
'G13.SI',
'S58.SI',
'BUOU.SI',
'H78.SI',
'U14.SI',
'S68.SI',
'D01.SI',
'C6L.SI',
'S63.SI',
'C07.SI',
'J36.SI',
]

session = Session()
# just add headers to your session and provide it to the reader
session.headers = {'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:89.0) Gecko/20100101 Firefox/89.0',
                   'Accept': 'application/json;charset=utf-8'}

for ticker in tickers:
    tickerName=ticker.split(".")[0]


    s=DataReader(
              ticker,
              'yahoo',
             "2021-10-1", "2022-04-01")

    s = s.reset_index() 

    print(s)

    s.to_sql("{ticker}".format(ticker=tickerName), con=engine, index=False)

connection.close()
engine.dispose()


