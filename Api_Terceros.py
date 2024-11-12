import requests

def obtener_datos_api_externa(ciudad):
    key = 'b9b664210634f1de44c25b915a76028f'
    url = f'http://api.openweathermap.org/data/2.5/weather?q={ciudad}&appid={key}'

    # Hacer la solicitud GET a la API
    response = requests.get(url)
    
    if response.status_code == 200:  # Si la solicitud fue exitosa
        data = response.json()

        # Extraer la información que necesitas
        fecha = data['dt']  # Fecha como timestamp Unix
        ciudad = data['name']
        temp_maxima = data['main']['temp_max']
        temp_minima = data['main']['temp_min']

        return fecha, ciudad, temp_maxima, temp_minima
    else:
        print("Error al obtener los datos de la API externa")
        return None
