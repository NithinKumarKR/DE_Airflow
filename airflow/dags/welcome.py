from airflow import DAG

from airflow.operators.python import PythonOperator

import pendulum

from datetime import datetime

import requests



def print_welcome():

    print('Welcome to Airflow!')



def print_date():

    print('Today is {}'.format(datetime.today().date()))






dag = DAG(

    'welcome',

    default_args={'start_date': pendulum.now().subtract(days=1)},

    schedule='0 23 * * *',

    catchup=False

)



print_welcome_task = PythonOperator(

    task_id='print_welcome',

    python_callable=print_welcome,

    dag=dag

)



print_date_task = PythonOperator(

    task_id='print_date',

    python_callable=print_date,

    dag=dag

)






# Set the dependencies between the tasks

print_welcome_task  >> print_date_task

