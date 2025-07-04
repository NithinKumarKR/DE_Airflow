from airflow import DAG
from datetime import datetime
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.decorators import task


mysql_conn_id = 'mysql_airflow'
api_conn_id = 'open_meteo_api'

with DAG(
    dag_id="etl_pipeline",
    start_date=datetime(2025, 5, 26),
    schedule='*/10 * * * *',
    catchup=False,
    tags=['weather', 'etl']
):

    @task
    def lat_log():
        values = []
        for i in range(1, 2):   
            lat = float(10 + i * 0.1)
            lon = float(20 + i * 0.5)
            values.append({"latitude": lat, "longitude": lon})
        return values  # ✅ This return is critical for mapping

    @task
    def extract_weather_data(cordinate):
        http_hook = HttpHook(http_conn_id=api_conn_id, method="GET")
        latitude = cordinate['latitude']
        longitude = cordinate['longitude']
        endpoint = f"/v1/forecast?latitude={latitude}&longitude={longitude}&current_weather=true"
        response = http_hook.run(endpoint)

        if response.status_code == 200:
            data = response.json()
            data['latitude'] = latitude  # keep track of lat/lon for transformation
            data['longitude'] = longitude
            return data
        else:
            raise Exception(f"Failed to fetch weather data: {response.status_code}")

    @task
    def transform_weather_data(data):
        current_weather = data['current_weather']
        return {
            'latitude': data['latitude'],
            'longitude': data['longitude'],
            'temperature': current_weather['temperature'],
            'windspeed': current_weather['windspeed'],
            'winddirection': current_weather['winddirection'],
            'weathercode': current_weather['weathercode']
        }

    @task
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
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
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

    # ✅ Task dependency flow with .expand() for dynamic mapping
    coordinates = lat_log()
    weather_data = extract_weather_data.expand(cordinate=coordinates)
    transformed = transform_weather_data.expand(data=weather_data)
    load_weather_data.expand(transformed_data=transformed)
