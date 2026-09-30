import json
import os
from datetime import datetime

from kafka import KafkaConsumer
import mysql.connector
from dotenv import load_dotenv


# ============================================================
# LOAD ENVIRONMENT VARIABLES
# ============================================================

load_dotenv()


# ============================================================
# CONFIGURATION
# ============================================================

KAFKA_BROKER = "localhost:9092"

TRAFFIC_TOPIC = "traffic_data"
WEATHER_TOPIC = "weather_data"

KAFKA_GROUP = "traffic-weather-consumer"


MYSQL_HOST = os.getenv("MYSQL_HOST", "localhost")
MYSQL_PORT = int(os.getenv("MYSQL_PORT", "3306"))
MYSQL_USER = os.getenv("MYSQL_USER", "root")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD")
MYSQL_DATABASE = "traffic_sda"


# ============================================================
# HEADER
# ============================================================

print("\n" + "=" * 80)
print("REAL-TIME TRAFFIC + WEATHER STREAMING CONSUMER")
print("=" * 80)

print(f"Kafka Broker : {KAFKA_BROKER}")
print(f"Traffic Topic: {TRAFFIC_TOPIC}")
print(f"Weather Topic: {WEATHER_TOPIC}")
print(f"MySQL DB     : {MYSQL_DATABASE}")

print("=" * 80)


# ============================================================
# MYSQL CONNECTION
# ============================================================

if not MYSQL_PASSWORD:
    raise RuntimeError(
        "MYSQL_PASSWORD not found in .env file."
    )


db = mysql.connector.connect(
    host=MYSQL_HOST,
    port=MYSQL_PORT,
    user=MYSQL_USER,
    password=MYSQL_PASSWORD,
    database=MYSQL_DATABASE
)

cursor = db.cursor()

print("✓ MySQL connection successful")


# ============================================================
# KAFKA CONSUMER
# ============================================================

consumer = KafkaConsumer(
    TRAFFIC_TOPIC,
    WEATHER_TOPIC,

    bootstrap_servers=[KAFKA_BROKER],

    group_id=KAFKA_GROUP,

    auto_offset_reset="latest",

    enable_auto_commit=True,

    value_deserializer=lambda x:
        json.loads(x.decode("utf-8"))
)


print("✓ Kafka consumer connected")
print("✓ Listening to traffic_data + weather_data")
print("\nWaiting for streaming events...\n")


# ============================================================
# STORE LATEST WEATHER BY LOCATION
# ============================================================

latest_weather = {}


# ============================================================
# HELPER FUNCTION
# ============================================================

def parse_datetime(value):

    if value is None:
        return None

    try:

        value = str(value)

        # Handle ISO timestamps ending in Z
        value = value.replace("Z", "+00:00")

        dt = datetime.fromisoformat(value)

        # MySQL DATETIME does not need timezone
        return dt.replace(tzinfo=None)

    except Exception:

        return None


# ============================================================
# INSERT COMBINED EVENT
# ============================================================

def insert_event(traffic, weather):

    trip_id = traffic.get("Trip_ID")

    start_area = traffic.get("start_area")

    end_area = traffic.get("end_area")

    stream_timestamp = parse_datetime(
        traffic.get("stream_timestamp")
    )

    weather_timestamp = None

    if weather:

        weather_timestamp = parse_datetime(
            weather.get("timestamp")
        )

    query = """
        INSERT INTO traffic_weather_events (

            trip_id,
            event_timestamp,
            weather_timestamp,

            start_area,
            end_area,

            distance_km,
            time_of_day,
            day_of_week,

            historical_weather_condition,

            traffic_density_level,
            road_type,

            average_speed_kmph,
            travel_time_minutes,

            live_temperature_c,
            live_feels_like_c,
            live_humidity_pct,
            live_rain_mm,
            live_precipitation_mm,
            live_cloud_cover_pct,
            live_visibility_m,
            live_wind_speed_kmh,
            live_weather_code,

            stream_timestamp,

            source

        )

        VALUES (

            %s, %s, %s,
            %s, %s,
            %s, %s, %s,
            %s,
            %s, %s,
            %s, %s,
            %s, %s, %s,
            %s, %s, %s,
            %s, %s, %s,
            %s,
            %s

        )
    """

    values = (

        trip_id,

        stream_timestamp,

        weather_timestamp,

        start_area,

        end_area,

        traffic.get("distance_km"),

        traffic.get("time_of_day"),

        traffic.get("day_of_week"),

        traffic.get("weather_condition"),

        traffic.get("traffic_density_level"),

        traffic.get("road_type"),

        traffic.get("average_speed_kmph"),

        traffic.get("travel_time_minutes"),

        weather.get("temperature_c") if weather else None,

        weather.get("feels_like_c") if weather else None,

        weather.get("humidity_pct") if weather else None,

        weather.get("rain_mm") if weather else None,

        weather.get("precipitation_mm") if weather else None,

        weather.get("cloud_cover_pct") if weather else None,

        weather.get("visibility_m") if weather else None,

        weather.get("wind_speed_kmh") if weather else None,

        weather.get("weather_code") if weather else None,

        stream_timestamp,

        traffic.get(
            "source",
            "Delhi NCR Traffic Dataset"
        )

    )

    cursor.execute(query, values)

    db.commit()


# ============================================================
# MAIN STREAM PROCESSING
# ============================================================

try:

    for message in consumer:

        event = message.value

        # ====================================================
        # WEATHER EVENT
        # ====================================================

        if message.topic == WEATHER_TOPIC:

            location = event.get("location")

            latest_weather[location] = event

            print("\n" + "-" * 80)

            print("🌦️ WEATHER EVENT RECEIVED")

            print(
                f"Location      : {location}"
            )

            print(
                f"Temperature   : "
                f"{event.get('temperature_c')} °C"
            )

            print(
                f"Humidity      : "
                f"{event.get('humidity_pct')} %"
            )

            print(
                f"Rain          : "
                f"{event.get('rain_mm')} mm"
            )

            print(
                f"Precipitation : "
                f"{event.get('precipitation_mm')} mm"
            )

            print(
                f"Wind Speed    : "
                f"{event.get('wind_speed_kmh')} km/h"
            )

            print("✓ Weather cache updated")


        # ====================================================
        # TRAFFIC EVENT
        # ====================================================

        elif message.topic == TRAFFIC_TOPIC:

            start_area = event.get("start_area")

            weather = latest_weather.get(
                start_area
            )

            print("\n" + "-" * 80)

            print("🚦 TRAFFIC EVENT RECEIVED")

            print(
                f"Trip ID       : "
                f"{event.get('Trip_ID')}"
            )

            print(
                f"Route         : "
                f"{event.get('start_area')} → "
                f"{event.get('end_area')}"
            )

            print(
                f"Traffic       : "
                f"{event.get('traffic_density_level')}"
            )

            print(
                f"Speed         : "
                f"{event.get('average_speed_kmph')} km/h"
            )

            print(
                f"Travel Time   : "
                f"{event.get('travel_time_minutes')} min"
            )


            # =================================================
            # WEATHER ENRICHMENT
            # =================================================

            if weather:

                print(
                    f"🌦️ Weather     : "
                    f"{weather.get('temperature_c')} °C | "
                    f"{weather.get('precipitation_mm')} mm"
                )

                print(
                    "✓ Traffic event enriched with live weather"
                )

            else:

                print(
                    "⚠️ No live weather available "
                    f"for {start_area}"
                )


            # =================================================
            # STORE IN MYSQL
            # =================================================

            insert_event(
                event,
                weather
            )

            print(
                "✓ Combined event stored in MySQL"
            )


except KeyboardInterrupt:

    print(
        "\n\n🛑 Consumer stopped by user."
    )


finally:

    cursor.close()

    db.close()

    consumer.close()

    print(
        "✓ Kafka consumer closed."
    )

    print(
        "✓ MySQL connection closed."
    )