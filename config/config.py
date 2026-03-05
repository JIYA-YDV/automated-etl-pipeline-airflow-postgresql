# config/config.py
import os
from dotenv import load_dotenv

load_dotenv()

# API Configuration
OPENWEATHER_API_KEY = os.getenv("OPENWEATHER_API_KEY")
BASE_URL = "http://api.openweathermap.org/data/2.5/weather"

# Cities to monitor
CITIES = [
    "Kathmandu",
    "Delhi",
    "London",
    "New York",
    "Tokyo"
]

# Database Configuration
DB_CONFIG = {
    "host": os.getenv("DB_HOST", "localhost"),
    "port": os.getenv("DB_PORT", 5432),
    "dbname": os.getenv("DB_NAME", "weather_pipeline"),
    "user": os.getenv("DB_USER", "postgres"),
    "password": os.getenv("Ln@jy")
}