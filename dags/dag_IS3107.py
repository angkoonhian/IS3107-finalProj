# portfolio opt packages
import cvxpy as cvx
# from pypfopt import EfficientFrontier
# from pypfopt import risk_models
# from pypfopt import expected_returns

# importing packages
from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
import pandas as pd
from pandas_datareader.data import DataReader

from requests import Request, Session

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
session.headers = {'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:89.0) Gecko/20100101 Firefox/89.0',
                   'Accept': 'application/json;charset=utf-8'}

# predefine method for getting stock data
def get_data_for_multiple_stocks(tickers, start_date, end_date):
    '''
    Function that uses Pandas DataReader to download data directly from Yahoo Finance,
    computes the Log Returns series for each ticker, and returns a DataFrame 
    containing the Log Returns of all specified tickers.
    
    Parameters:
    - tickers (list): List of Stock Tickers.
    - start_date, end_date (str): Start and end dates in the format 'YYYY-MM-DD'.
    
    Returns:
    - returns_df (pd.DataFrame): A DataFrame with dates as indexes, and columns corresponding
                                 to the log returns series of each ticker.
    '''

    # initialise output dataframe
    returns_df = pd.DataFrame()
    
    for ticker in tickers:
        # retrieve stock data (includes Date, OHLC, Volume, Adjusted Close)
        s = DataReader(ticker, 'yahoo', datetime.date(datetime.now()) - timedelta(days=365), datetime.date(datetime.now()))
        # calculate log returns
        s['Log Returns'] = np.log(s['Adj Close']/s['Adj Close'].shift(1))
        # append to returns_df
        returns_df[ticker] = s['Log Returns']
        
    # skip the first row (that will be NA)
    # and fill other NA values by 0 in case there are trading halts on specific days
    returns_df = returns_df.iloc[1:].fillna(0)
        
    return returns_df
    
# Standard optimization with cvxpy
    
def get_optimized_portfolio(returns_df, returns_scale = .0001, max_holding = 0.5):
        """
        Function that takes in the returns series of assets, minimizes the utility function, 
        and returns the portfolio weights

        Parameters
        ----------
        returns_df : pd.dataframe
            Dataframe containing log asset return series in each column

        returns_scale : float
            The scaling factor applied to the returns

        max_holding : float
            The maximum weight a stock can hold.

        Returns
        -------
        x : np.ndarray
            A numpy ndarray containing the weights of the assets in the optimized portfolio
        """

        # convert returns dataframe to numpy array
        returns = returns_df.T.to_numpy()
        # m is the number of assets
        m = returns.shape[0]
        print(m)

        # covariance matrix of returns
        cov = np.cov(returns)
        print(returns)

        # creating variable of weights to optimize
        x = cvx.Variable(m)
        print(x)

        # portfolio variance, in quadratic form
        portfolio_variance = cvx.quad_form(x, cov)
        print("return in simple returns")
        print(returns_df)
        log_returns_df = np.log(returns_df+1)
        print("return in log returns")
        print(log_returns_df)
        total_return_log = log_returns_df.sum().to_numpy() #this is in log space, change to simple return
        print("total return in log")
        print(total_return_log)
        print("total simple return")

        total_simple_return = np.exp(total_return_log) -1
        print(total_simple_return)
        frequency = 252 #assume daily compounding, we are going to take geometric average
        #this is the standard basic mean for optimization (to assume daily compounding)

        horizon_length = returns.shape[1]
        expected_mean = (1 + total_simple_return) ** (1 / horizon_length) - 1
        print("geometric return")
        print(expected_mean)
        #let's assume 
        # element wise multiplication, followed up by sum of weights
        portfolio_return = sum(cvx.multiply(expected_mean, x))

        # Objective Function
        # We want to minimize variance and maximize returns. We can also minimize the negative of returns.
        # Therefore, variance has to be a positive and returns have to be a negative.
        objective = cvx.Minimize(portfolio_variance - returns_scale * portfolio_return)

        # Constraints
        # long only, sum of weights equal to 1, no allocation to a single stock great than 50% of portfolio
        constraints = [x >= 0, sum(x) == 1, x <= max_holding]

        # use cvxpy to solve the objective
        problem = cvx.Problem(objective, constraints)
        # retrieve the weights of the optimized portfolio
        result = problem.solve()

        return x.value
        
# predefine portfolio opt methods
def portfolio_opt():

    returns_df = get_data_for_multiple_stocks(tickers, datetime.date(datetime.now()) - timedelta(days=365), datetime.date(datetime.now()))
    # extract previous output with xcom
    simple_returns = simple_returns = np.exp(returns_df) -1
    # define in-sample period (for optimization) and out-of-sample period (for evaluation)
    in_sample = "2020-01-01"
    is_returns_df = simple_returns.loc[:in_sample]
    oos_returns_df = simple_returns.loc[in_sample:][1:] # one day after in_sample date
    print(simple_returns)
    print("??")
    # Set max holdings to 1, no limits
    example = get_optimized_portfolio(simple_returns, returns_scale = 0, max_holding = 1)
    return round(pd.Series(example, index = is_returns_df.columns), 2)

with DAG(
        'IS3107_final',
        # pass in default args
        default_args = {
            'depends_on_past': True,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=1),
            },
        description='Final Project IS3107',
        schedule_interval=timedelta(minutes=1),
        start_date=datetime(2022,4,10),
        catchup=True,
        tags=['IS3107']
        ) as dag:
        
        # get_market_data = PythonOperator(task_id='get_market_data', python_callable=get_data_for_multiple_stocks(['AAPL'], '2022-03-03', '2022-03-05'), dag=dag)
       
        
        get_opt_portfolio = PythonOperator(task_id='get_opt_portfolio', python_callable=portfolio_opt)
        
        get_opt_portfolio