from airflow import DAG
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.decorators import task
import pendulum

# Replace with actual values
latitude = '24'
longitude = '24'

mysql_conn_id = 'mysql_airflow'
api_conn_id = 'open_meteo_api'

default_args = {
    'owner': 'airflow',
    'start_date': pendulum.now().subtract(days=1),
}

with DAG(
    dag_id="weather_etl_pipeline",
    default_args=default_args,
    schedule='0 23 * * *',
    catchup=False,
    tags=['weather', 'etl']
) as dag:

    @task()
    def extract_weather_data(latitude):
        http_hook = HttpHook(http_conn_id=api_conn_id, method="GET")
        endpoint = f"/v1/forecast?latitude={latitude}&longitude={longitude}&current_weather=true"
        response = http_hook.run(endpoint)
        
        if response.status_code == 200:
            return response.json()
        else:
            raise Exception(f"Failed to fetch weather data: {response.status_code}")

    @task()
    def transform_weather_data(weather_data):
        current_weather = weather_data['current_weather']
        return {
            'latitude': latitude,
            'longitude': longitude,
            'temperature': current_weather['temperature'],
            'windspeed': current_weather['windspeed'],
            'winddirection': current_weather['winddirection'],
            'weathercode': current_weather['weathercode']
        }

    @task()
    def load_weather_data(transformed_data):
        mysql_hook = MySqlHook(mysql_conn_id=mysql_conn_id)
        conn = mysql_hook.get_conn()
        cursor = conn.cursor()

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS weather_data (
                latitude FLOAT,
                longitude FLOAT,
                temperature FLOAT,
                windspeed FLOAT,
                winddirection FLOAT,
                weathercode INT,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP ,
                        PRIMARY KEY (latitude, longitude)

            );
        """)

        cursor.execute("""
            INSERT INTO weather_data (latitude, longitude, temperature, windspeed, winddirection, weathercode)
            VALUES (%s, %s, %s, %s, %s, %s);
        """, (
            transformed_data['latitude'],
            transformed_data['longitude'],
            transformed_data['temperature'],
            transformed_data['windspeed'],
            transformed_data['winddirection'],
            transformed_data['weathercode']
        ))

        conn.commit()
        cursor.close()
        conn.close()
    for i in range(0,20,10):
        weather_data = extract_weather_data(i)
        transformed = transform_weather_data(weather_data)
        load_weather_data(transformed)
