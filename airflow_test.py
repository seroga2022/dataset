import airflow
import pandas as pd
import datetime as dt
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

dag = DAG(
 dag_id="download_cars_test",
 start_date=dt.datetime(2022, 7, 17),
 schedule_interval=None,
)

def get_cars():
    df = pd.read_csv('cars.csv')
    print(df.columns)
    
def save_database():
    None


get_cars = PythonOperator(
    task_id="get file cars.csv",
    python_callable=get_cars,
    dag=dag,
)

save_database = PythonOperator(
    task_id="save to database",
    python_callable=save_database,
    dag=dag,
)

get_cars>>save_database

