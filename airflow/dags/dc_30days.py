from airflow import DAG
from airflow.decorators import task
from airflow.providers.mysql.hooks.mysql import MySqlHook
import pendulum

mysql_conn_id = 'mysql_airflow'

default_args = {
    'owner': 'airflow',
    'start_date': pendulum.now().subtract(days=1),
}

with DAG(
    dag_id="clearing_30_days",
    default_args=default_args,
    schedule='0 23 * * *',
    catchup=False,
) as dag:

    @task()
    def reading_tables():
        mysql_hook = MySqlHook(mysql_conn_id=mysql_conn_id)
        conn = mysql_hook.get_conn()
        cursor = conn.cursor()

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS clearing_30_days (
                id INT AUTO_INCREMENT ,
                tablename VARCHAR(255),
                clearing_type VARCHAR(255),
                column_primary_key VARCHAR(255),
                column_considered VARCHAR(255),
                created_name VARCHAR(255) DEFAULT 'nithin',
                created_id INT DEFAULT 1,
                created TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                lastedited_name VARCHAR(255) DEFAULT 'nithin',
                lastedited_id INT DEFAULT 1,
                lastedited TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP, 
                PRIMARY KEY (id, tablename))
        """)

        conn.commit()
        cursor.close()
        conn.close()

    @task()
    def source_reading_tables():
        mysql_hook = MySqlHook(mysql_conn_id=mysql_conn_id)
        conn = mysql_hook.get_conn()
        cursor = conn.cursor()

        cursor.execute("""
            select * from 
        """)

        conn.commit()
        cursor.close()
        conn.close()

    source_reading_tables()
    reading_tables()
