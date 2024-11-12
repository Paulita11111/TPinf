from flask import Flask, jsonify, request
import sqlite3

app = Flask(__name__)

DATABASE_NAME = "weather.db"

def get_db():
    connection = sqlite3.connect(DATABASE_NAME)
    return connection

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

if __name__ == '__main__':
    app.run(debug=True, port=5000)
