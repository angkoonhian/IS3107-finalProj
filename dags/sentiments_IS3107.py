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
from nltk.sentiment.vader import SentimentIntensityAnalyzer
import exchange_calendars as xcals
from bs4 import BeautifulSoup
import ujson
import numpy as np

def initialise_portfolio_info_data(port_ticks):
    all_info = []
    for ticker in port_ticks:
        sInfo = yf.Ticker(ticker).info
        all_info.append(sInfo)

    return pd.DataFrame(all_info)
    
def get_sentimental_analysis(info_df):
    name_sym = info_df[['longName', 'symbol']].copy()
    name_sym['longNameCaps'] = [x.upper() for x in list(name_sym['longName'])]
    name_sym['sentiment'] = ujson.dumps({})
    relevant_atags = []
    
    res = session.get('https://sginvestors.io/news/company-announcement/latest/')
    soup = BeautifulSoup(res.text, 'html.parser')
    
    all_ann = soup.find_all("div", class_="corpannouncementitem list-group-item")
    all_corp_ann = soup.select_one("#corporate-announcements")
    a_tags_corp_ann = all_corp_ann.find_all('a')
    
    today = datetime.today().strftime("%Y-%m-%d")
    for tag in a_tags_corp_ann:
#         print(tag.find('div', class_="corpann_info").getText().split(' ')[2])
        if tag.find('span', class_='corpann_stock').getText() in list(name_sym['longNameCaps']):
#             if tag.find('div', class_="corpann_info").getText().split(' ')[2] == today: 
            relevant_atags.append(tag)
    
    print('==========relevant_tags========', relevant_atags)
    
    
    relevant_sentimental_analysis = []
    for relevant_tag in relevant_atags:
        corp_name = relevant_tag.find('span', class_='corpann_stock').getText()
        desc = relevant_tag.find('span', class_="corpann_descr").getText()
#         print('desc', desc)
        lyzer = SentimentIntensityAnalyzer()
        tag_sentiment = lyzer.polarity_scores(desc)
#         print(ujson.dumps(tag_sentiment))
#         name_sym.loc[name_sym['longNameCaps'] == corp_name]['sentiment'] = ujson.dumps(tag_sentiment)
        #['sentiment'] = lyzer.polarity_scores(desc)
        name_sym['sentiment'] = np.where(name_sym['longNameCaps'] == corp_name, ujson.dumps(tag_sentiment), name_sym['sentiment'])
    
    
    return name_sym


def sentiment_analysis():

	xses = xcals.get_calendar('XSES')
	last_trading_day = xses.date_to_session('2022-04-03', direction='previous').strftime("%Y-%m-%d")
	last_trading_day
	xses.is_session(datetime.today().strftime("%Y-%m-%d"))
	portfolio_tickers = [ (str(s)[str(s).find('(') + 1 : str(s).find(')')].split(':'))[-1] + '.SI' for s in list(pd.read_html('https://sginvestors.io/analysts/sti-straits-times-index-constituents-target-price')[0]['Straits Times Index STI Constituent'])]
	session = requests_cache.CachedSession(cache_name='cache', backend='sqlite')
	# just add headers to your session and provide it to the reader
	session.headers = {'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:89.0) Gecko/20100101 Firefox/89.0',
		           'Accept': 'application/json;charset=utf-8'}
	portfolio_tickers.remove('na.SI')
	dff = yf.download(' '.join(portfolio_tickers))
	yf.Tickers(portfolio_tickers).info
	dff.to_csv('./yf_data_downloaded.csv')
	for ticker in portfolio_tickers:
	    DataReader(ticker, 'yahoo', end=last_trading_day, session=session).to_csv(f'./yf_data/{last_trading_day}_{ticker}.csv')
	pd.read_html('https://sginvestors.io/analysts/sti-straits-times-index-constituents-target-price')[0]
	testList = portfolio_tickers[:5]
	u11 = yf.Ticker('U11.SI')
	session = requests_cache.CachedSession(cache_name='cache', backend='sqlite')
	# just add headers to your session and provide it to the reader
	session.headers = {'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:89.0) Gecko/20100101 Firefox/89.0',
		           'Accept': 'application/json;charset=utf-8'}
	info_df = initialise_portfolio_info_data(portfolio_tickers)
	name_sym = info_df[['longName', 'symbol']].copy()
	relLoc = name_sym.loc[name_sym['longName']== 'United Overseas Bank Limited']
	# relLoc['symbol']
	name_sym['longNameCaps'] = [x.upper() for x in list(name_sym['longName'])]
	get_sentimental_analysis(info_df).to_csv('./test_sentimental_dailyData.csv')
	res.text
	soup = BeautifulSoup(res.text, 'html.parser')
	all_ann = soup.find_all("div", class_="corpannouncementitem list-group-item")
	# all_corp_ann = soup.find("div", {"id": "corporate-announcements"})
	all_corp_ann = soup.select_one("#corporate-announcements")
	a_tags_corp_ann = all_corp_ann.find_all('a')
	print(a_tags_corp_ann[0].find('div', class_="corpann_info").getText().split(' ')[2])
	# all_corp_ann_sgx_links = all_corp_ann.find_all('a')
	# all_corp_ann_sgx_links[0].find('i', {"class": "fa fa-clock-o"}).getText()
	desc = a_tags_corp_ann[1].find('span', class_="corpann_descr").getText()
	desc = a_tags_corp_ann[1].find('span', class_="corpann_descr").getText()
	print(soup.prettify())
	import ujson
	testD = {
	    'hi': "hi"
	}
	nltk.download('vader_lexicon')
	lyzer = SentimentIntensityAnalyzer()
	return lyzer.polarity_scores(desc)


with DAG(
        'IS3107_final_sentimssent',
        # pass in default args
        default_args = {
            'depends_on_past': True,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=1),
            },
        description='Final Project IS3107 Sentiment Analysis',
        schedule_interval=timedelta(minutes=1),
        start_date=datetime(2021,3,3),
        catchup=True,
        tags=['IS3107']
        ) as dag:
        
        # get_market_data = PythonOperator(task_id='get_market_data', python_callable=get_data_for_multiple_stocks(['AAPL'], '2022-03-03', '2022-03-05'), dag=dag)
        
        get_sentiment_analysis = PythonOperator(task_id='get_sentiment_analysis', python_callable=sentiment_analysis)
        
        trigger = TriggerDagRunOperator(
        task_id="test_trigger_dagrun",
        trigger_dag_id="example_trigger_target_dag",  # Ensure this equals the dag_id of the DAG to trigger
        conf={"message": "Hello World"},
    )
        
        get_sentiment_analysis