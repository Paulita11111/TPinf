import csv
from product import Report
import sqlite3
import pandas as pd

file_path = r'C:\Users\User\Downloads\europe_cities.csv'
df = pd.read_csv(file_path)

DATABASE_NAME = "weather.db"

def get_db():
    connection = sqlite3.connect(DATABASE_NAME)
    return connection

def create_tables():
    sql_create_table = """
    CREATE TABLE IF NOT EXISTS weather (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        date TEXT NOT NULL,
        city TEXT NOT NULL,
        temp_max REAL NOT NULL,
        temp_min REAL NOT NULL,
        humidity REAL NOT NULL
    );
    """
    db = get_db()
    cursor = db.cursor()
    cursor.execute(sql_create_table)
    db.commit()
    db.close()

create_tables()

def save_europe_cities(info):
    db = get_db()
    cursor = db.cursor()
    try:
        cursor.execute("""
            INSERT INTO weather (date, city, temp_max, temp_min, humidity)
            VALUES (?, ?, ?, ?, ?)
        """, (info['date'], info['city'], info['temp_max'], info['temp_min'], info['humidity']))
        db.commit()
    except Exception as e:
        print("Error al guardar datos:", e)
    finally:
        db.close()

def load_europe_cities():
    db = get_db()
    cursor = db.cursor()
    cursor.execute("SELECT date, city, temp_max, temp_min, humidity FROM weather")
    records = cursor.fetchall()
    db.close()
    
    return [Report(r[0], r[1], "Not provided", {"temp_max": r[2], "temp_min": r[3], "humidity": r[4]}) for r in records]

def europe_cities_average():
    cities_data = load_europe_cities()
    avg_temp_max = sum(float(report.weather_data['temp_max']) for report in cities_data) / len(cities_data)
    avg_temp_min = sum(float(report.weather_data['temp_min']) for report in cities_data) / len(cities_data)
    return {'avg_temp_max': avg_temp_max, 'avg_temp_min': avg_temp_min}


'''

DATABASE_NAME = "weather.db"  # Nombre de la base de datos

def get_db():
    connection = sqlite3.connect(DATABASE_NAME)
    return connection

def create_tables():
    # Crear la tabla 'clima' con las columnas
    sql_create_table = """
    CREATE TABLE IF NOT EXISTS clima (
        id INTEGER PRIMARY KEY AUTOINCREMENT,  -- Columna id para identificación única
        date TEXT NOT NULL,                   -- Fecha del registro
        city TEXT NOT NULL,                  -- Ciudad a la que corresponde el clima
        temp_max REAL NOT NULL,             -- Temperatura máxima
        temp_min REAL NOT NULL              -- Temperatura mínima
        humidity REAL NOT NULL              -- Humedad
    );
    """

    # Conexión a la base de datos y ejecución del SQL
    db = get_db()
    cursor = db.cursor()
    cursor.execute(sql_create_table)  # Ejecutar la consulta SQL
    db.commit()  # Confirmamos los cambios
    db.close()   # Cerramos la conexión

# Llamada para crear las tablas
create_tables()
'''
