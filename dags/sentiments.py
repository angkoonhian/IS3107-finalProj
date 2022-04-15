from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.contrib.hooks.snowflake_hook import SnowflakeHook
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
import pandas as pd


def get_sentimental_analysis():
    """
    Scrapes https://sginvestors.io/news/company-announcement/latest/ to get the
    corporate annoucements for each of our ticker's company, if any
    Returns a dataframe with tickers as the columns and indexed by today's date
    Returned data is to be inserted into table.
    """
    from bs4 import BeautifulSoup
    from requests import Request, Session
    import ujson
    import numpy as np
    import nltk
    nltk.download('vader_lexicon')
    from nltk.sentiment.vader import SentimentIntensityAnalyzer


    session = Session()
    
    info_df = pd.DataFrame.from_dict({'symbol': {0: 'U11.SI', 1: 'D05.SI', 2: 'C52.SI', 3: 'BN4.SI', 4: 'V03.SI', 5: 'C38U.SI', 6: 'A17U.SI', 7: 'Z74.SI', 8: 'O39.SI', 9: 'N2IU.SI', 10: 'Y92.SI', 11: 'F34.SI', 12: 'ME8U.SI', 13: 'AJBU.SI', 14: 'C09.SI', 15: 'M44U.SI', 16: 'U96.SI', 17: '9CI.SI', 18: 'BS6.SI', 19: 'G13.SI', 20: 'S58.SI', 21: 'BUOU.SI', 22: 'H78.SI', 23: 'U14.SI', 24: 'S68.SI', 25: 'D01.SI', 26: 'C6L.SI', 27: 'S63.SI', 28: 'C07.SI', 29: 'J36.SI'}, 'longName': {0: 'United Overseas Bank Limited', 1: 'DBS Group Holdings Ltd', 2: 'ComfortDelGro Corporation Limited', 3: 'Keppel Corporation Limited', 4: 'Venture Corporation Limited', 5: 'CapitaLand Integrated Commercial Trust', 6: 'Ascendas Real Estate Investment Trust', 7: 'Singapore Telecommunications Limited', 8: 'Oversea-Chinese Banking Corporation Limited', 9: 'Mapletree Commercial Trust', 10: 'Thai Beverage Public Company Limited', 11: 'Wilmar International Limited', 12: 'Mapletree Industrial Trust', 13: 'Keppel DC REIT', 14: 'City Developments Limited', 15: 'Mapletree Logistics Trust', 16: 'Sembcorp Industries Ltd', 17: 'CapitaLand Investment Limited', 18: 'Yangzijiang Shipbuilding (Holdings) Ltd.', 19: 'Genting Singapore Limited', 20: 'SATS Ltd.', 21: 'Frasers Logistics & Commercial Trust', 22: 'Hongkong Land Holdings Limited', 23: 'UOL Group Limited', 24: 'Singapore Exchange Limited', 25: 'Dairy Farm International Holdings Limited', 26: 'Singapore Airlines Limited', 27: 'Singapore Technologies Engineering Ltd', 28: 'Jardine Cycle & Carriage Limited', 29: 'Jardine Matheson Holdings Limited'}, 'shortName': {0: 'UOB', 1: 'DBS', 2: 'ComfortDelGro', 3: 'Keppel Corp', 4: 'Venture', 5: 'CapLand IntCom T', 6: 'Ascendas Reit', 7: 'Singtel', 8: 'OCBC Bank', 9: 'Mapletree Com Tr', 10: 'ThaiBev', 11: 'Wilmar Intl', 12: 'Mapletree Ind Tr', 13: 'Keppel DC Reit', 14: 'CityDev', 15: 'Mapletree Log Tr',
                                     16: 'Sembcorp Ind', 17: 'CapitaLandInvest', 18: 'YZJ Shipbldg SGD', 19: 'Genting Sing', 20: 'SATS', 21: 'FRASERS LOGISTICS & COM TRUST', 22: 'HongkongLand USD', 23: 'UOL', 24: 'SGX', 25: 'DairyFarm USD', 26: 'SIA', 27: 'ST Engineering', 28: 'Jardine C&C', 29: 'JMH USD'}, 'financialCurrency': {0: 'SGD', 1: 'SGD', 2: 'SGD', 3: 'SGD', 4: 'SGD', 5: 'SGD', 6: 'SGD', 7: 'SGD', 8: 'SGD', 9: 'SGD', 10: 'THB', 11: 'USD', 12: 'SGD', 13: 'SGD', 14: 'SGD', 15: 'SGD', 16: 'SGD', 17: 'SGD', 18: 'CNY', 19: 'SGD', 20: 'SGD', 21: 'SGD', 22: 'USD', 23: 'SGD', 24: 'SGD', 25: 'USD', 26: 'SGD', 27: 'SGD', 28: 'USD', 29: 'USD'}, 'industry': {0: 'Banks—Regional', 1: 'Banks—Regional', 2: 'Railroads', 3: 'Conglomerates', 4: 'Electronic Components', 5: 'REIT—Retail', 6: 'REIT—Industrial', 7: 'Telecom Services', 8: 'Banks—Regional', 9: 'REIT—Retail', 10: 'Beverages—Wineries & Distilleries', 11: 'Farm Products', 12: 'REIT—Industrial', 13: 'REIT—Office', 14: 'Real Estate—Development', 15: 'REIT—Industrial', 16: 'Conglomerates', 17: 'Real Estate Services', 18: 'Aerospace & Defense', 19: 'Resorts & Casinos', 20: 'Airports & Air Services', 21: 'REIT—Industrial', 22: 'Real Estate—Development', 23: 'Real Estate—Development', 24: 'Financial Data & Stock Exchanges', 25: 'Grocery Stores', 26: 'Airlines', 27: 'Aerospace & Defense', 28: 'Auto Manufacturers', 29: 'Conglomerates'}, 'country': {0: 'Singapore', 1: 'Singapore', 2: 'Singapore', 3: 'Singapore', 4: 'Singapore', 5: 'Singapore', 6: 'Singapore', 7: 'Singapore', 8: 'Singapore', 9: 'Singapore', 10: 'Thailand', 11: 'Singapore', 12: 'Singapore', 13: 'Singapore', 14: 'Singapore', 15: 'Singapore', 16: 'Singapore', 17: 'Singapore', 18: 'China', 19: 'Singapore', 20: 'Singapore', 21: 'Singapore', 22: 'Bermuda', 23: 'Singapore', 24: 'Singapore', 25: 'Hong Kong', 26: 'Singapore', 27: 'Singapore', 28: 'Singapore', 29: 'Bermuda'}})
    today = datetime.today().strftime("%Y-%m-%d")
    print(today)
    name_sym = info_df[['longName', 'symbol']].copy()
    name_sym['longNameCaps'] = [x.upper() for x in list(name_sym['longName'])]
    name_sym['sentiment'] = ujson.dumps({})
    name_sym['date'] = today
    relevant_atags = []
    relevant_sentimental_analysis = []
    for i in range(1, 6):
        res_text = session.get(
            f'https://sginvestors.io/news/company-announcement/latest/{i}').text
        soup = BeautifulSoup(res_text, 'html.parser')

        all_ann = soup.find_all(
            "div", class_="corpannouncementitem list-group-item")
        all_corp_ann = soup.select_one("#corporate-announcements")
        a_tags_corp_ann = all_corp_ann.find_all('a')

        for tag in a_tags_corp_ann:
            if tag.find('span', class_='corpann_stock').getText() in list(name_sym['longNameCaps']):
                if tag.find('div', class_="corpann_info").getText().split(' ')[2] == today:
                    relevant_atags.append(tag)

    for relevant_tag in relevant_atags:
        corp_name = relevant_tag.find('span', class_='corpann_stock').getText()
        desc = relevant_tag.find('span', class_="corpann_descr").getText()
        lyzer = SentimentIntensityAnalyzer()
        tag_sentiment = lyzer.polarity_scores(desc)
        name_sym['sentiment'] = np.where(name_sym['longNameCaps'] == corp_name, ujson.dumps(
            tag_sentiment), name_sym['sentiment'])

    psdf = name_sym[['symbol', 'sentiment']]
    fresh_sdf = pd.DataFrame(columns=psdf['symbol'], index=[
        today], data=psdf.to_dict())

    sentiment_string_series = "'"+psdf['sentiment']+"'"

    values_str = '(Date, ' + ', '.join(fresh_sdf.columns) + ')'
    sql_string = "insert into sentimental.public.sentimentals "
    # sql_string += values_str

    inside_str = f"values(TO_DATE('{today}', 'YYYY-MM-DD'), " + \
        ', '.join(sentiment_string_series) + ')'
    sql_string += inside_str

    return sql_string


def retrieve_today_senti_send_email():
    import smtplib
    today = datetime.today().strftime("%Y-%m-%d")
    sql_string = f'select * from sentimental.public.sentimentals s where "Date"=to_date(\'{today}\',\'YYYY-MM-DD\') LIMIT 1'
    snowflakeHook = SnowflakeHook(snowflake_conn_id="SnowflakeConnection",
        warehouse='COMPUTE_WH',
        database='SENTIMENTALS',
        schema='PUBLIC',
        role='ACCOUNTADMIN',)
    df = snowflakeHook.get_pandas_df(sql_string)

    message = """\
        Subject: Airflow Analysis

        This message is sent from IS3107 Group 6"""
    
    
    for ticker, sentiment in df.iteritems():
        if ticker == 'Date':
            continue
        
        if sentiment[0] != '{}':
            message += f'{ticker}: {sentiment[0]} \n'

    try:
        # Create your SMTP session
        smtp = smtplib.SMTP('smtp.gmail.com', 587)

    # Use TLS to add security
        smtp.starttls()

        # User Authentication
        smtp.login("airflowgrp6", "Pee@ssword1")

        # Defining The Message
        

        # Sending the Email
        smtp.sendmail("airflowgrp6@gmail.com", "airflowgrp6@gmail.com", message)

        # Terminating the session
        smtp.quit()
        print("Email sent successfully!")

    except Exception as ex:
        print("Something went wrong....", ex)
    return


with DAG(
        'Sentiments',
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
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 4, 13),
        catchup=False,
        tags=['IS3107']
) as dag:

    sql_string = get_sentimental_analysis()
    print(sql_string)

    snowflake_sentimental_operator = SnowflakeOperator(
        task_id='insert_senti_data',
        sql=sql_string,
        snowflake_conn_id='SnowflakeConnection',
        warehouse='COMPUTE_WH',
        database='SENTIMENTALS',
        schema='PUBLIC',
        role='ACCOUNTADMIN',
    )

    send_email = PythonOperator(
        task_id='send_sentiment_email', python_callable=retrieve_today_senti_send_email)

    snowflake_sentimental_operator >> send_email
