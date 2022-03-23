from airflow import DAG
from airflow.operators.python import PythonOperator, PythonVirtualenvOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator 

from datetime import datetime


def choose_stock(ti):
    data = ti.xcom_pull(task_ids="download_yf")
    spy_data = data['Close']['SPY']
    tsla_data = data['Close']['TSLA']

    spy =  (spy_data[-1] - spy_data[0])/spy_data[0]
    print(f'spy: {spy}')
    
    tsla =  (tsla_data[-1] - tsla_data[0])/tsla_data[0]
    print(f'tsla: {tsla}')

    if spy > tsla:
        return 'spy'
    return 'tsla'


def download_yf():
    import yfinance as yf
    data = yf.download("SPY TSLA", start="2020-01-01", end="2021-01-01", group="tickers")
    return data

with DAG("DAGGYDAGGY", start_date=datetime(2021, 1, 1), description='An e2e stock filter flow',
    schedule_interval="@daily", catchup=False) as dag:

    download_yf = PythonOperator(
        task_id="download_yf",
        python_callable=download_yf,
        dag=dag
        )
    
    choose_stock = BranchPythonOperator(
        task_id="choose_stock",
        python_callable=choose_stock
    )

    spy = BashOperator(
        task_id="spy",
        bash_command="echo 'buy spy'"
    )

    tsla = BashOperator(
        task_id="tsla",
        bash_command="echo 'buy tsla'"
    )


    # dependency has to match operators name
    download_yf >> choose_stock >> [spy, tsla]