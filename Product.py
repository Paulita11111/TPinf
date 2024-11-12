class Report:
    def __init__(self, date, city, current_weather, weather_data) -> None:
        self.date = date
        self.city = city
        self.current_weather = current_weather
        self.weather_data = weather_data

    def serialize(self):
        return{
            'Date': self.date,
            'City': self.city,
            'Current Weather': self.current_weather,
            'Weather Data': self.weather_data
        }

class Report_Average(Report):
    def __init__(self, date, city, current_weather, weather_data, average_weather, average_data) -> None:
        super().__init__(date, city, current_weather, weather_data)
        self.average_weather = average_weather
        self.average_data = average_data


    def serialize_average(self):
        return {
            'Average Weather': self.average_weather,
            'Average Data': self.average_data
                    }
