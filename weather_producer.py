import json
import time
import requests

from datetime import datetime
from kafka import KafkaProducer


# ============================================================
# CONFIGURATION
# ============================================================

KAFKA_BROKER = "localhost:9092"
KAFKA_TOPIC = "weather_data"

OPEN_METEO_URL = "https://api.open-meteo.com/v1/forecast"


# ============================================================
# DELHI NCR LOCATIONS
# ============================================================

LOCATIONS = {
    "Connaught Place": {
        "lat": 28.6315,
        "lon": 77.2167
    },

    "ITO": {
        "lat": 28.6280,
        "lon": 77.2410
    },

    "Rohini": {
        "lat": 28.7495,
        "lon": 77.0565
    },

    "Gurgaon CyberHub": {
        "lat": 28.4949,
        "lon": 77.0895
    },

    "Noida Sector 18": {
        "lat": 28.5708,
        "lon": 77.3260
    }
}


# ============================================================
# CREATE KAFKA PRODUCER
# ============================================================

producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BROKER],
    value_serializer=lambda value: json.dumps(value).encode("utf-8")
)


# ============================================================
# FETCH LIVE WEATHER
# ============================================================

def fetch_weather(location_name, coordinates):

    params = {
        "latitude": coordinates["lat"],
        "longitude": coordinates["lon"],

        "current": (
            "temperature_2m,"
            "relative_humidity_2m,"
            "apparent_temperature,"
            "precipitation,"
            "rain,"
            "showers,"
            "weather_code,"
            "cloud_cover,"
            "visibility,"
            "wind_speed_10m"
        ),

        "timezone": "Asia/Kolkata"
    }

    response = requests.get(
        OPEN_METEO_URL,
        params=params,
        timeout=10
    )

    response.raise_for_status()

    data = response.json()

    current = data["current"]

    weather_event = {
        "source": "Open-Meteo",

        "location": location_name,

        "latitude": coordinates["lat"],
        "longitude": coordinates["lon"],

        "timestamp": current["time"],

        "temperature_c": current["temperature_2m"],

        "feels_like_c": current["apparent_temperature"],

        "humidity_pct": current["relative_humidity_2m"],

        "precipitation_mm": current["precipitation"],

        "rain_mm": current["rain"],

        "showers_mm": current["showers"],

        "weather_code": current["weather_code"],

        "cloud_cover_pct": current["cloud_cover"],

        "visibility_m": current["visibility"],

        "wind_speed_kmh": current["wind_speed_10m"]
    }

    return weather_event


# ============================================================
# STREAM WEATHER TO KAFKA
# ============================================================

def stream_weather():

    print("\n" + "=" * 70)
    print("🌦️  DELHI NCR LIVE WEATHER STREAM")
    print("=" * 70)

    print(f"📡 Kafka Broker : {KAFKA_BROKER}")
    print(f"📂 Kafka Topic  : {KAFKA_TOPIC}")
    print("🌐 Source       : Open-Meteo Live Weather API")

    print("=" * 70)

    while True:

        for location_name, coordinates in LOCATIONS.items():

            try:

                weather_event = fetch_weather(
                    location_name,
                    coordinates
                )

                producer.send(
                    KAFKA_TOPIC,
                    value=weather_event
                )

                producer.flush()

                print("\n🌦️  WEATHER EVENT")
                print("-" * 70)

                print(
                    f"📍 Location       : "
                    f"{weather_event['location']}"
                )

                print(
                    f"🕐 Timestamp      : "
                    f"{weather_event['timestamp']}"
                )

                print(
                    f"🌡️  Temperature    : "
                    f"{weather_event['temperature_c']} °C"
                )

                print(
                    f"🤒 Feels Like     : "
                    f"{weather_event['feels_like_c']} °C"
                )

                print(
                    f"💧 Humidity       : "
                    f"{weather_event['humidity_pct']} %"
                )

                print(
                    f"🌧️  Rain          : "
                    f"{weather_event['rain_mm']} mm"
                )

                print(
                    f"☔ Precipitation   : "
                    f"{weather_event['precipitation_mm']} mm"
                )

                print(
                    f"☁️  Cloud Cover    : "
                    f"{weather_event['cloud_cover_pct']} %"
                )

                print(
                    f"👁️  Visibility     : "
                    f"{weather_event['visibility_m']} m"
                )

                print(
                    f"💨 Wind Speed     : "
                    f"{weather_event['wind_speed_kmh']} km/h"
                )

                print(
                    f"🌤️  Weather Code   : "
                    f"{weather_event['weather_code']}"
                )

                print("-" * 70)

                print("✓ Published → weather_data")

            except requests.exceptions.RequestException as error:

                print(
                    f"\n❌ API error for "
                    f"{location_name}: {error}"
                )

            except Exception as error:

                print(
                    f"\n❌ Error for "
                    f"{location_name}: {error}"
                )

            # Small delay between locations
            time.sleep(2)

        print(
            "\n⏳ All 5 locations updated."
        )

        print(
            "⏳ Waiting 60 seconds before next cycle...\n"
        )

        time.sleep(60)


# ============================================================
# START PRODUCER
# ============================================================

if __name__ == "__main__":

    try:

        stream_weather()

    except KeyboardInterrupt:

        print("\n\n🛑 Weather producer stopped by user.")

    finally:

        producer.flush()
        producer.close()

        print("✓ Kafka producer closed.")
