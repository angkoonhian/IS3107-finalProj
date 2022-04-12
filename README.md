# IS3107-finalProj

## Start Up 
1. Create an python env : <code>python3 venv env</code>
2. Run the env <code>source env/bin/activate</code>
3. Install dependencies <code>pip install -r requirements.txt</code>
4. Init DB <code>airflow db init</code>
5. Init a superuser <br/>
  <code>airflow users create \
--username admin \
--firstname firstName \
--lastname lastName \
--role Admin \
--email email@email.com</code>
6. Run Airflow <code>airflow webserver --port 8080</code>
7. Run Scheduler <code>airflow scheduler</code>
8. Login

## Snowflake Connection

#### Admin > Connections > Add Connection

Connection ID : SnowflakeConnection <br />
Connection Type : Snowflake<br />
Host : oq82740.ap-southeast-1.snowflakecomputing.com<br />
schema : STI_DAILY_RAW_DATA<br />
login : weilin<br />
password : P@ssword1<br />
Account : oq82740<br />
Database : PORTFOLIO_REBALANCING<br />
Region : ap-southeast-1<br/>
Warehouse : COMPUTE_WH<br />
