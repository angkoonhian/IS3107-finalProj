import numpy as np
import ujson
from bs4 import BeautifulSoup
import exchange_calendars as xcals
from nltk.sentiment.vader import SentimentIntensityAnalyzer
from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
import yfinance as yf
import pandas as pd
from pandas_datareader.data import DataReader
from datetime import datetime
import requests
import requests_cache
import nltk
nltk.download('vader_lexicon')


def initialise_portfolio_info_data(port_ticks):
    """
    Retrieves data from YFinance
    Returns info_df to be passed into get_sentimental_analysis
    """
    all_info = []
    for ticker in port_ticks:
        sInfo = yf.Ticker(ticker).info
        all_info.append(sInfo)

    return pd.DataFrame(all_info)


portfolio_tickers = ['U11.SI', 'D05.SI', 'C52.SI',
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

info_df = initialise_portfolio_info_data(portfolio_tickers)


def get_sentimental_analysis():
    """
    Scrapes https://sginvestors.io/news/company-announcement/latest/ to get the 
    corporate annoucements for each of our ticker's company, if any
    Returns a dataframe with tickers as the columns and indexed by today's date
    Returned data is to be inserted into table.
    """
    global info_df
    today = datetime.today().strftime("%Y-%m-%d")
    name_sym = info_df[['longName', 'symbol']].copy()
    name_sym['longNameCaps'] = [x.upper() for x in list(name_sym['longName'])]
    name_sym['sentiment'] = ujson.dumps({})
    name_sym['date'] = today
    relevant_atags = []

    res = session.get(
        'https://sginvestors.io/news/company-announcement/latest/')
    soup = BeautifulSoup(res.text, 'html.parser')

    all_ann = soup.find_all(
        "div", class_="corpannouncementitem list-group-item")
    all_corp_ann = soup.select_one("#corporate-announcements")
    a_tags_corp_ann = all_corp_ann.find_all('a')

    for tag in a_tags_corp_ann:
        if tag.find('span', class_='corpann_stock').getText() in list(name_sym['longNameCaps']):
            if tag.find('div', class_="corpann_info").getText().split(' ')[2] == today:
                relevant_atags.append(tag)

    relevant_sentimental_analysis = []
    for relevant_tag in relevant_atags:
        corp_name = relevant_tag.find('span', class_='corpann_stock').getText()
        desc = relevant_tag.find('span', class_="corpann_descr").getText()
        lyzer = SentimentIntensityAnalyzer()
        tag_sentiment = lyzer.polarity_scores(desc)
        name_sym['sentiment'] = np.where(name_sym['longNameCaps'] == corp_name, ujson.dumps(
            tag_sentiment), name_sym['sentiment'])

    psdf = name_sym[['symbol', 'sentiment']]
    fresh_sdf = pd.DataFrame(columns=sdf['symbol'], index=[
                             today], data=psdf.to_dict())

    return fresh_sdf  # then insert into snowflake db


with DAG(
        'IS3107_final_sentimssent',
        # pass in default args
        default_args={
            'depends_on_past': True,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=1),
        },
        description='Final Project IS3107 Sentiment Analysis',
        schedule_interval=timedelta(minutes=1),
        start_date=datetime(2022, 04, 12),
        catchup=True,
        tags=['IS3107']
) as dag:

    get_sentiment_analysis = PythonOperator(
        task_id='get_sentiment_analysis', python_callable=get_sentiment_analysis)

    trigger = TriggerDagRunOperator(
        task_id="test_trigger_dagrun",
        # Ensure this equals the dag_id of the DAG to trigger
        trigger_dag_id="example_trigger_target_dag",
        conf={"message": "Hello World (sentimentally)"},
    )

    get_sentiment_analysis
