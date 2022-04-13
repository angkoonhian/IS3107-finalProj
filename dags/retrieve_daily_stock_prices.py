from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from airflow.contrib.hooks.snowflake_hook import SnowflakeHook
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator

import pandas as pd
from requests import Request, Session
from pandas_datareader.data import DataReader


session = Session()

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

# just add headers to your session and provide it to the reader
session.headers = {
    'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:89.0) Gecko/20100101 Firefox/89.0',
    'Accept': 'application/json;charset=utf-8'}

with DAG("RETRIEVE_DAILY_STOCK_PRICES", default_args={'depends_on_past': True,
                                                      'email': ['airflow@example.com'],
                                                      'email_on_failure': False,
                                                      'email_on_retry': False,
                                                      'retries': 1,
                                                      'retry_delay': timedelta(minutes=1)}, description='Final Project IS3107-Daily Extraction',
         schedule_interval="00 22 * * 1-5",
         start_date=datetime(2022, 4, 10),
         catchup=True,
         tags=['Extract']) as dag:

    for ticker in tickers:
        tickerName = ticker.split(".")[0]

        currentDate = datetime.today().strftime('%Y-%m-%d')

        s = DataReader(
            ticker,
            'yahoo',
            currentDate, currentDate)

        snowflakeQuery = [
            "INSERT INTO '{tickerName}' VALUES(CURRENT_DATE(), {high}, {low}, {open}, {close}, {volume}, {adjClose})".format(
                tickerName=tickerName,
                high=s.iloc[0]["High"],
                low=s.iloc[0]["Low"],
                open=s.iloc[0]["Open"],
                close=s.iloc[0]["Close"],
                volume=s.iloc[0]["Volume"],
                adjClose=s.iloc[0]["Adj Close"])]

        snowflakeOp = SnowflakeOperator(
            task_id='extract_{tickerName}'.format(
                tickerName=tickerName),
            sql=snowflakeQuery,
            snowflake_conn_id="SnowflakeConnection", schema="STI_DAILY_RAW_DATA"
          )

        snowflakeOp
