import pandas as pd
import sqlite3

file_path = r'C:\Users\User\Downloads\europe_cities.csv'
DATABASE_NAME = "weather.db"

def get_db():
    """Establece una conexión con la base de datos."""
    try:
        connection = sqlite3.connect(DATABASE_NAME)
        return connection
    except sqlite3.Error as e:
        print("Error al conectar con la base de datos:", e)
        return None

def create_weather_table():
    """Crea la tabla 'weather' si no existe."""
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
    if db is not None:
        try:
            cursor = db.cursor()
            cursor.execute(sql_create_table)
            db.commit()
            print("Tabla 'weather' creada o ya existe.")
        except sqlite3.Error as e:
            print("Error al crear la tabla:", e)
        finally:
            db.close()

def load_csv_to_db():
    """Carga datos desde el archivo CSV a la base de datos, si la tabla 'weather' está vacía."""
    try:
        df = pd.read_csv(file_path)
    except FileNotFoundError:
        print("Error: archivo CSV no encontrado en la ruta especificada.")
        return

    db = get_db()
    if db is not None:
        try:
            cursor = db.cursor()
            # Verifica si la tabla ya tiene datos
            cursor.execute("SELECT COUNT(*) FROM weather")
            if cursor.fetchone()[0] == 0:
                # Inserta datos desde el CSV
                for _, row in df.iterrows():
                    cursor.execute("""
                        INSERT INTO weather (date, city, temp_max, temp_min, humidity)
                        VALUES (?, ?, ?, ?, ?)
                    """, (row['date'], row['city'], row['temp_max'], row['temp_min'], row['humidity']))
                db.commit()
                print("Datos cargados exitosamente desde el CSV a la base de datos.")
            else:
                print("La base de datos ya tiene datos, no se cargó el CSV.")
        except sqlite3.Error as e:
            print("Error al cargar datos desde el CSV:", e)
        finally:
            db.close()

def is_city_in_csv(city_name):
    """Verifica si la ciudad existe en el archivo CSV."""
    try:
        df = pd.read_csv(file_path)
        return city_name in df['city'].values
    except FileNotFoundError:
        print("Error: archivo CSV no encontrado en la ruta especificada.")
        return False

def add_city_weather_to_db(city_name, date, temp_max, temp_min, humidity):
    """Agrega datos de clima para una ciudad en la base de datos."""
    db = get_db()
    if db is not None:
        try:
            cursor = db.cursor()
            cursor.execute("""
                INSERT INTO weather (date, city, temp_max, temp_min, humidity)
                VALUES (?, ?, ?, ?, ?)
            """, (date, city_name, temp_max, temp_min, humidity))
            db.commit()
            print(f"Datos de clima agregados para la ciudad {city_name}.")
        except sqlite3.Error as e:
            print("Error al agregar datos a la base de datos:", e)
        finally:
            db.close()

# Crear la tabla si no existe
create_weather_table()

# Cargar datos del CSV a la base de datos si es necesario
load_csv_to_db()
