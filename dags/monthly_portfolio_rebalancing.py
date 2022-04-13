# portfolio opt packages
import cvxpy as cvx

# importing packages
from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from airflow.contrib.hooks.snowflake_hook import SnowflakeHook
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
import pandas as pd
from pandas_datareader.data import DataReader


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
'\"9CI\"',
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

# predefine method for getting stock data
def retrieveAndCalculateLogReturns(**kwargs):

    currentMonth = datetime.now().month
    currentYear = datetime.now().year
    
    # initialise output dataframe
    logReturnsPd = pd.DataFrame()

    snowflakeHook = SnowflakeHook(snowflake_conn_id="SnowflakeConnection", schema="STI_DAILY_RAW_DATA")

    for name in tableNames :
        sqlResult = snowflakeHook.get_pandas_df("SELECT * FROM '{name}' WHERE DATADATE MONTH({currentMonth} AND YEAR({currentYear}))".format(name=name, currentMonth=currentMonth, currentYear={currentYear}))

        sqlResults["Log Returns"] = np.log(s['ADJCLOSE']/s['ADJCLOSE'].shift(1))

        logReturnsPd[name] = s["Log Returns"]

    # skip the first row (that will be NA)
    # and fill other NA values by 0 in case there are trading halts on specific days
    logReturnsPd = logReturnsPd.iloc[1:].fillna(0)

    return logReturnsPd

    
# predefine portfolio opt methods
def portfolio_opt(**kwargs):

    # extract previous output with xcom
    ti = kwargs['ti']

    logReturnsDf =  ti.xcom_pull(task_ids='fetch_monthly_log_returns')
    simple_returns = np.exp(logReturnsDf) -1


    # Set max holdings to 1, no limits
    optPortfolio = get_optimized_portfolio(simple_returns, returns_scale = 0, max_holding = 1)

    optPortfolioInstance = kwargs["optPortfolioInstance"]
    optPortfolioInstance.xcom_push(key="optPortSeries", value=optPortfolio)

    print(optPortfolio)

    return round(pd.Series(optPortfolio, index = simple_returns.columns), 2)



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
        catchup=False,
        tags=['Monthly Portfolio Balancing']
        ) as dag:       
        
        retrieve_and_calculate_log_returns = PythonOperator(task_id="fetch_monthly_log_returns", python_callable=retrieveAndCalculateLogReturns)
        get_opt_portfolio = PythonOperator(task_id='get_opt_portfolio', python_callable=portfolio_opt)

        
        retrieve_and_calculate_log_returns >> get_opt_portfolio


        optPortfolioSeries = optPortfolioInstance.xcom.pull(task_id="get_opt_portfolio", key="optPortSeries")

        for index, value in optPortfolioSeries.items() :

            snowflakeHook = SnowflakeHook(snowflake_conn_id="SnowflakeConnection", schema="MONTHLY_PORTFOLIO_BALANCES")

            lastMonth = snowflakeHook.get_first("SELECT TOP 1 * FROM {tickerName} ORDER BY ID DESC".format(tickerName=index))

            print(lastMonth)

            difference = value - lastMonth["Weights"] 

            uploadQuery = ["CREATE TABLE IF NOT EXISTS {tickerName} (date DATE, Weights (FLOAT) Difference From Prev (FLOAT)".format(
            tickerName=index), "INSERT INTO {tickerName} VALUES(CURRENT_DATE(), {weight}, {difference})".format(tickerName=index, weight=value, difference={difference})]

            snowflakeOp = SnowflakeOperator(  task_id='upload_{tickerName}'.format(
                tickerName=tickerName),
            sql=sqlQuery,
            snowflake_conn_id="SnowflakeConnection", schema="MONTHLY_PORTFOLIO_BALANCES")


            get_opt_portfolio >> snowflakeOp
