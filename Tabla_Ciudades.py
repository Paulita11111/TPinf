import pandas as pd
import sqlite3

file_path = r'C:\Users\User\Downloads\europe_cities.csv'
DATABASE_NAME = "weather.db"

def get_db():
    connection = sqlite3.connect(DATABASE_NAME)
    return connection

def create_weather_table():
    sql_create_table = """
    CREATE TABLE IF NOT EXISTS weather (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        date TEXT,
        city TEXT NOT NULL,
        temp_max REAL,
        temp_min REAL,
        humidity REAL
    );
    """
    db = get_db()
    cursor = db.cursor()
    cursor.execute(sql_create_table)
    db.commit()
    db.close()

def load_csv_to_db():
    df = pd.read_csv(file_path)
    db = get_db()
    cursor = db.cursor()

    # Insert data if the table is empty
    cursor.execute("SELECT COUNT(*) FROM weather")
    if cursor.fetchone()[0] == 0:
        for _, row in df.iterrows():
            cursor.execute("""
                INSERT INTO weather (date, city, temp_max, temp_min, humidity)
                VALUES (?, ?, ?, ?, ?)
            """, (row['date'], row['city'], row['temp_max'], row['temp_min'], row['humidity']))
        db.commit()
        print("Datos cargados exitosamente desde el CSV a la base de datos.")
    else:
        print("La base de datos ya tiene datos, no se cargó el CSV.")

    db.close()

# Crear la tabla si no existe
create_weather_table()

# Cargar datos del CSV a la base de datos si es necesario
load_csv_to_db()
