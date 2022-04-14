# portfolio opt packages
import cvxpy as cvx

# importing packages
from airflow import DAG
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from airflow.contrib.hooks.snowflake_hook import SnowflakeHook
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
import pandas as pd
import numpy as np
from pandas_datareader.data import DataReader
from pandas.tseries.offsets import BMonthEnd
from airflow.models import Variable as VAR



tableNames = ['U11', 'D05', 'C52',
'BN4',
'V03',
'C38U',
'A17U',
'Z74',
'O39',
'N2IU',
'Y92',
'F34',
'ME8U',
'AJBU',
'C09',
'M44U',
'U96',
'9CI',
'BS6',
'G13',
'S58',
'BUOU',
'H78',
'U14',
'S68',
'D01',
'C6L',
'S63',
'C07',
'J36',
]

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


        # covariance matrix of returns
        cov = np.cov(returns)


        # creating variable of weights to optimize
        x = cvx.Variable(m)

        # portfolio variance, in quadratic form
        portfolio_variance = cvx.quad_form(x, cov)

        log_returns_df = np.log(returns_df+1)

        total_return_log = log_returns_df.sum().to_numpy() #this is in log space, change to simple return

        total_simple_return = np.exp(total_return_log) -1
        print(total_simple_return)
        frequency = 252 #assume daily compounding, we are going to take geometric average
        #this is the standard basic mean for optimization (to assume daily compounding)

        horizon_length = returns.shape[1]
        expected_mean = (1 + total_simple_return) ** (1 / horizon_length) - 1

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

# predefine method for getting stock data
def retrieveAndCalculateLogReturns(**kwargs):

    currentMonth = datetime.now().month
    currentYear = datetime.now().year
    
    # initialise output dataframe
    logReturnsPd = pd.DataFrame()

    snowflakeHook = SnowflakeHook(snowflake_conn_id="SnowflakeConnection", schema="STI_DAILY_RAW_DATA", database="PORTFOLIO_REBALANCING" )

    for name in tableNames :
        sqlResult = snowflakeHook.get_pandas_df("SELECT * FROM 'S_{name}' WHERE MONTH(date) = {currentMonth} AND YEAR(date) = {currentYear}".format(name=name, currentMonth=currentMonth, currentYear={currentYear}))

        ## since april got no data yet, i queried for march data
        # sqlResult = snowflakeHook.get_pandas_df('SELECT * FROM S_{name} ticker WHERE MONTH("DataDate") = 3 AND YEAR("DataDate") = 2022'.format(name=name))

        sqlResult["Log Returns"] = np.log(sqlResult['Adj Close']/sqlResult['Adj Close'].shift(1))

        logReturnsPd[name] = sqlResult["Log Returns"]

    # skip the first row (that will be NA)
    # and fill other NA values by 0 in case there are trading halts on specific days
    logReturnsPd = logReturnsPd.iloc[1:].fillna(0)

    return logReturnsPd.to_json()

    
# predefine portfolio opt methods
def portfolio_opt(**kwargs):

    # extract previous output with xcom
    ti = kwargs['ti']

    logReturnsJson =  ti.xcom_pull(task_ids='fetch_monthly_log_returns')
    logReturnsDf = pd.read_json(logReturnsJson)
    simple_returns = np.exp(logReturnsDf) -1

    # Set max holdings to 1, no limits
    optPortfolio = get_optimized_portfolio(simple_returns, returns_scale = 0, max_holding = 1)

    print(round(pd.Series(optPortfolio, index=simple_returns.columns), 5).to_json())

    VAR.set(key="optPortfolio", value=round(pd.Series(optPortfolio, index=simple_returns.columns), 5).to_json())

    return round(pd.Series(optPortfolio, index=simple_returns.columns), 5).to_json()


def retrieve_previous_portfolio(tickerName, value) :

    snowflakeHook = SnowflakeHook(snowflake_conn_id="SnowflakeConnection", schema="MONTHLY_PORTFOLIO_BALANCES",database="PORTFOLIO_REBALANCING" )  
    lastMonth = snowflakeHook.get_first('SELECT TOP 1 * FROM {tickerName} ORDER BY "DataDate" DESC'.format(tickerName=tickerName))

    print(lastMonth[1])
    print(value)

    difference = value - lastMonth[1] 

    return difference


with DAG(
        'Monthly_Rebalancing',
        # pass in default args
        default_args = {
            'depends_on_past': True,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=1),
            },
        description='portfolio_rebalancing_batch',
        schedule_interval="0 8 24-31 * 3",
        start_date=datetime(2022,4,10),
        catchup=False,
        tags=['Monthly Portfolio Balancing']
        ) as dag:       
            
        
        retrieve_and_calculate_log_returns = PythonOperator(task_id="fetch_monthly_log_returns", python_callable=retrieveAndCalculateLogReturns)
        get_opt_portfolio = PythonOperator(task_id='get_opt_portfolio', python_callable=portfolio_opt)
        

        optPortfolioJson = VAR.get('optPortfolio',
                                 default_var="{}",
                                 deserialize_json=False)

        
        optPortfolioSeries= pd.read_json(optPortfolioJson, typ='series', orient='records')

        
        with TaskGroup('upload_data',
                    prefix_group_id=False,
                    ) as upload_data:
            

            for index, value in optPortfolioSeries.items() :

                tickerName = "S_" + index

                differenceFloat = retrieve_previous_portfolio(tickerName, value)
                print(differenceFloat)

                uploadQuery = ["CREATE TABLE IF NOT EXISTS {tickerName} (DataDate TIMESTAMP_NTZ(9), Weights FLOAT, DifferenceFromPrev FLOAT)".format(
                tickerName=tickerName), "INSERT INTO {tickerName} VALUES(CURRENT_DATE(), {weight}, {difference})".format(tickerName=tickerName, weight=value, difference=differenceFloat)]

                snowflakeOp = SnowflakeOperator(task_id='upload_{tickerName}'.format(
                   tickerName=tickerName),
                sql=uploadQuery,
                snowflake_conn_id="SnowflakeConnection", schema="MONTHLY_PORTFOLIO_BALANCES", database="PORTFOLIO_REBALANCING" )

                snowflakeOp

        retrieve_and_calculate_log_returns >> get_opt_portfolio >> upload_data
