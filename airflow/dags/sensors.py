from airflow import DAG ,Dataset
from airflow.decorators import Dataset
from airflow.sensors.filesystem import FileSensor
from datetime import datetime



with DAG('sensors_working',
         schedule='@daily' ,

         )