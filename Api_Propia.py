from flask import Flask, jsonify, request
import sqlite3
import requests
import pandas as pd

app = Flask(__name__)

DATABASE_NAME = "weather.db"
file_path = r'C:\Users\User\Downloads\europe_cities.csv'  # Ruta al archivo CSV con ciudades de Europa
API_KEY = 'tu_api_key_de_clima'  # Reemplaza con tu clave de API
WEATHER_API_URL = 'https://api.openweathermap.org/data/2.5/weather'

def get_db():
    connection = sqlite3.connect(DATABASE_NAME)
    return connection

def is_city_in_csv(city_name):
    df = pd.read_csv(file_path)
    return city_name in df['city'].values

@app.route('/', methods=['GET'])
def index():
    return "Welcome to the Home Page of The Weather API!"

@app.route('/hello', methods=['GET'])
def hello():
    return "Welcome to The Weather API!"

@app.route("/api/weather/europe_cities", methods=['GET'])
def get_all_europe_cities():
    db = get_db()
    cursor = db.cursor()
    try:
        cursor.execute("SELECT * FROM weather")
        records = cursor.fetchall()
        weather_data = [{"id": r[0], "date": r[1], "city": r[2], "temp_max": r[3], "temp_min": r[4]} for r in records]
        return jsonify(weather_data)
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        db.close()

@app.route('/api/weather/europe_cities', methods=['POST'])
def create_europe_cities():
    new_data = request.json
    db = get_db()
    cursor = db.cursor()
    try:
        cursor.execute("""
            INSERT INTO weather (date, city, temp_max, temp_min)
            VALUES (?, ?, ?, ?)
        """, (new_data['date'], new_data['city'], new_data['temp_max'], new_data['temp_min']))
        db.commit()
        return jsonify({"message": "Weather data added successfully"}), 201
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        db.close()

@app.route("/api/weather/europe_cities/reports/<date>", methods=['GET'])
def get_weather_stat_by_date(date):
    db = get_db()
    cursor = db.cursor()
    try:
        cursor.execute("SELECT temp_max, temp_min FROM weather WHERE date = ?", (date,))
        records = cursor.fetchall()

        if not records:
            return jsonify({"error": "No data available for the given date"}), 404

        temp_max = [r[0] for r in records]
        temp_min = [r[1] for r in records]
        average_max_temp = sum(temp_max) / len(temp_max)
        average_min_temp = sum(temp_min) / len(temp_min)
        
        return jsonify({
            "date": date,
            "average_max_temp": average_max_temp,
            "average_min_temp": average_min_temp
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        db.close()

# Nuevo endpoint para obtener el clima de una ciudad específica
@app.route("/api/weather/city/<city_name>", methods=['GET'])
def get_weather_by_city(city_name):
    if not is_city_in_csv(city_name):
        return jsonify({"error": "City not listed in Europe cities CSV"}), 404

    db = get_db()
    cursor = db.cursor()
    
    try:
        # Revisa si la ciudad ya está en la base de datos
        cursor.execute("SELECT * FROM weather WHERE city = ?", (city_name,))
        result = cursor.fetchone()

        if result:
            # Si la ciudad ya tiene datos, devolverlos
            weather_data = {
                "id": result[0],
                "date": result[1],
                "city": result[2],
                "temp_max": result[3],
                "temp_min": result[4],
                "humidity": result[5]
            }
            return jsonify(weather_data)
        else:
            # Si la ciudad no está en la base de datos, consulta la API
            response = requests.get(WEATHER_API_URL, params={
                'q': city_name,
                'appid': API_KEY,
                'units': 'metric'
            })
            data = response.json()

            if response.status_code == 200:
                # Extrae los datos y guarda en la base de datos
                weather_data = {
                    "date": data['dt'],
                    "city": data['name'],
                    "temp_max": data['main']['temp_max'],
                    "temp_min": data['main']['temp_min'],
                    "humidity": data['main']['humidity']
                }
                cursor.execute("""
                    INSERT INTO weather (date, city, temp_max, temp_min, humidity)
                    VALUES (?, ?, ?, ?, ?)
                """, (weather_data["date"], weather_data["city"], weather_data["temp_max"], weather_data["temp_min"], weather_data["humidity"]))
                db.commit()

                return jsonify(weather_data), 201
            else:
                return jsonify({"error": "City not found or API error"}), 404
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        db.close()

if __name__ == '__main__':
    app.run(debug=True, port=5000)

