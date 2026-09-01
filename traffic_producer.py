import json
import time
import pandas as pd

from datetime import datetime
from kafka import KafkaProducer


# ============================================================
# CONFIGURATION
# ============================================================

KAFKA_BROKER = "localhost:9092"
KAFKA_TOPIC = "traffic_data"

FEATURES_FILE = "data/delhi_traffic_features.csv"
TARGET_FILE = "data/delhi_traffic_target.csv"

# Delay between individual traffic events
STREAM_DELAY = 3


# ============================================================
# CREATE KAFKA PRODUCER
# ============================================================

producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BROKER],

    key_serializer=lambda key: key.encode("utf-8"),

    value_serializer=lambda value: json.dumps(
        value,
        default=str
    ).encode("utf-8")
)


# ============================================================
# LOAD TRAFFIC DATA
# ============================================================

def load_traffic_data():

    print("\n📂 Loading traffic datasets...")

    features = pd.read_csv(FEATURES_FILE)
    target = pd.read_csv(TARGET_FILE)

    print(f"✓ Traffic features loaded: {len(features)} rows")
    print(f"✓ Traffic target loaded:   {len(target)} rows")

    # Combine both datasets using Trip_ID
    traffic_data = features.merge(
        target,
        on="Trip_ID",
        how="left"
    )

    print(
        f"✓ Combined traffic dataset: "
        f"{len(traffic_data)} rows"
    )

    return traffic_data


# ============================================================
# CLEAN DATA VALUES
# ============================================================

def clean_value(value):

    if pd.isna(value):
        return None

    if hasattr(value, "item"):
        try:
            return value.item()
        except Exception:
            pass

    return value


# ============================================================
# STREAM TRAFFIC DATA
# ============================================================

def stream_traffic():

    traffic_data = load_traffic_data()

    print("\n" + "=" * 75)
    print("🚦 DELHI NCR REAL-TIME TRAFFIC STREAM")
    print("=" * 75)

    print(f"📡 Kafka Broker : {KAFKA_BROKER}")
    print(f"📂 Kafka Topic  : {KAFKA_TOPIC}")
    print("📊 Source       : Delhi NCR Traffic Sample Dataset")
    print(f"⏱️  Event Delay : {STREAM_DELAY} seconds")

    print("=" * 75)

    while True:

        for _, row in traffic_data.iterrows():

            try:

                # ------------------------------------------------
                # CREATE TRAFFIC EVENT
                # ------------------------------------------------

                event = {}

                # Include every dataset column
                for column in traffic_data.columns:

                    event[column] = clean_value(
                        row[column]
                    )

                # Add streaming timestamp
                event["stream_timestamp"] = (
                    datetime.now().isoformat()
                )

                # Identify source
                event["source"] = (
                    "Delhi NCR Traffic Dataset"
                )

                # ------------------------------------------------
                # GET TRIP ID
                # ------------------------------------------------

                trip_id = str(
                    event.get("Trip_ID")
                )

                # ------------------------------------------------
                # SEND EVENT TO KAFKA
                # ------------------------------------------------

                producer.send(
                    KAFKA_TOPIC,
                    key=trip_id,
                    value=event
                )

                producer.flush()

                # ------------------------------------------------
                # DISPLAY EVENT
                # ------------------------------------------------

                print("\n🚦 TRAFFIC EVENT")
                print("-" * 75)

                print(
                    f"🆔 Trip ID        : "
                    f"{event.get('Trip_ID')}"
                )

                print(
                    f"📍 Route          : "
                    f"{event.get('start_area')} → "
                    f"{event.get('end_area')}"
                )

                print(
                    f"🚗 Traffic Density: "
                    f"{event.get('traffic_density_level')}"
                )

                print(
                    f"🛣️  Road Type      : "
                    f"{event.get('road_type')}"
                )

                print(
                    f"🏎️  Average Speed  : "
                    f"{event.get('average_speed_kmph')} km/h"
                )

                print(
                    f"⏱️  Travel Time    : "
                    f"{event.get('travel_time_minutes')} minutes"
                )

                print(
                    f"🕐 Stream Time    : "
                    f"{event.get('stream_timestamp')}"
                )

                print("-" * 75)

                print("✓ Published → traffic_data")

                # ------------------------------------------------
                # SIMULATE REAL-TIME STREAMING
                # ------------------------------------------------

                time.sleep(STREAM_DELAY)

            except Exception as error:

                print(
                    f"\n❌ Error processing "
                    f"Trip ID {row.get('Trip_ID')}: "
                    f"{error}"
                )

        # --------------------------------------------------------
        # RESTART STREAM
        # --------------------------------------------------------

        print("\n" + "=" * 75)

        print("🔄 Traffic dataset completed.")
        print("🔄 Restarting stream from first event...")

        print("=" * 75)

        time.sleep(5)


# ============================================================
# START PRODUCER
# ============================================================

if __name__ == "__main__":

    try:

        stream_traffic()

    except KeyboardInterrupt:

        print(
            "\n\n🛑 Traffic producer stopped by user."
        )

    finally:

        producer.flush()
        producer.close()

        print("✓ Kafka producer closed.")