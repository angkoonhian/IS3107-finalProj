# IS3107-finalProj

## Start Up 
1. Create an python env : <code>python3 venv env</code>
2. Run the env <code>source env/bin/activate</code>
3. Install dependencies <code>pip install -r requirements.txt</code>
4. Set Airflow's dag directory to <FOLDER_DIR>/dags
5. Init DB <code>airflow db init</code>
6. Init a superuser <br/>
  <code>airflow users create \
--username admin \
--firstname firstName \
--lastname lastName \
--role Admin \
--email email@email.com</code>
6. Run Airflow <code>airflow webserver --port 8080</code>
7. Run Scheduler <code>airflow scheduler</code>
8. Login
