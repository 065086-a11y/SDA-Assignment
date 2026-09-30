# Delhi NCR Real-Time Traffic & Weather Streaming Analytics

## Project Overview

This project implements a **real-time streaming analytics pipeline for Delhi NCR traffic monitoring** using Apache Kafka, Python, MySQL and Grafana.

The system processes traffic data as a continuous stream and integrates it with live weather information to provide insights into traffic congestion, travel time, vehicle speed, route performance and weather conditions.

The project demonstrates an end-to-end streaming architecture:

**Data Sources → Kafka → Python Consumer → Weather Enrichment → MySQL → Grafana Dashboard**

---

## 1. Problem Statement

Traffic congestion in Delhi NCR can significantly affect travel time, vehicle speed and route performance. Traditional batch-based analysis may not provide sufficiently timely information for monitoring changing traffic conditions.

The objective of this project is to build a **real-time traffic and weather monitoring system** that can:

* Monitor traffic events continuously
* Track traffic density and vehicle speed
* Analyse travel-time patterns
* Integrate live weather information
* Identify route-level performance
* Provide real-time dashboard visibility for decision-making

---

## 2. Data Sources

### Traffic Data

The project uses a Delhi NCR traffic dataset containing information such as:

* Trip ID
* Start Area
* End Area
* Distance
* Time of Day
* Day of Week
* Weather Condition
* Traffic Density Level
* Road Type
* Average Speed
* Travel Time

### Live Weather Data

Live weather information is obtained through the **Open-Meteo API**.

The weather stream includes:

* Temperature
* Feels-like temperature
* Humidity
* Rain
* Precipitation
* Cloud cover
* Visibility
* Wind speed
* Weather code

---

## 3. Technology Stack

| Technology     | Purpose                               |
| -------------- | ------------------------------------- |
| Python         | Data processing and streaming         |
| Apache Kafka   | Real-time message streaming           |
| Pandas         | Dataset processing                    |
| Open-Meteo API | Live weather data                     |
| MySQL          | Storage of processed streaming data   |
| Grafana        | Real-time dashboard and visualization |
| Docker         | Kafka infrastructure                  |

---

## 4. System Architecture

```text
                    TRAFFIC DATASET
                          |
                          v
                traffic_producer.py
                          |
                          v
                  Kafka: traffic_data
                          |
                          |
                          v
                    consumer.py
                          |
                          |
                          +----------------------+
                          |                      |
                          v                      v
                 Weather Enrichment          MySQL
                          |                      |
                          +-----------> traffic_weather_events
                                                 |
                                                 v
                                             Grafana
                                            Dashboard


                    LIVE WEATHER API
                          |
                          v
                weather_producer.py
                          |
                          v
                  Kafka: weather_data
                          |
                          v
                    consumer.py
```

---

## 5. Kafka Topics

The project uses two Kafka topics:

### `traffic_data`

Receives streaming traffic events generated from the Delhi NCR traffic dataset.

### `weather_data`

Receives live weather events obtained from the Open-Meteo API.

The consumer listens to both topics and combines the available weather information with incoming traffic events.

---

## 6. Project Files

```text
.
├── consumer.py
├── traffic_producer.py
├── weather_producer.py
├── delhi_traffic_features.csv
├── delhi_traffic_target.csv
├── requirements.txt
└── Assignment_3_dashboard code.json
```

### `traffic_producer.py`

Reads traffic data and continuously publishes traffic events to the Kafka `traffic_data` topic.

### `weather_producer.py`

Fetches live weather information and publishes weather events to the Kafka `weather_data` topic.

### `consumer.py`

Consumes traffic and weather events from Kafka, maintains the latest available weather information, enriches traffic events and stores the processed data in MySQL.

### `Assignment_3_dashboard code.json`

Contains the exported Grafana dashboard configuration/code used for the project dashboard.

---

## 7. Consumer and Data Processing

The Kafka consumer continuously listens to:

```text
traffic_data
weather_data
```

When a weather event is received, the consumer stores the latest weather information by location.

When a traffic event is received, the consumer checks for the latest available weather information for that location.

The processed event is then stored in MySQL.

```text
Traffic Event
      |
      v
Identify Start Area
      |
      v
Check Latest Weather
      |
      v
Weather Enrichment
      |
      v
Store Combined Event in MySQL
```

---

## 8. MySQL Database

The project uses the following MySQL database:

```text
Database: traffic_sda
```

The primary table used for dashboard analysis is:

```text
traffic_weather_events
```

The table stores traffic information along with available live weather attributes.

Key fields include:

```text
trip_id
event_timestamp
start_area
end_area
distance_km
traffic_density_level
average_speed_kmph
travel_time_minutes
live_temperature_c
live_precipitation_mm
live_humidity_pct
live_visibility_m
live_wind_speed_kmh
stream_timestamp
source
```

---

## 9. Grafana Dashboard

The processed data is connected to Grafana to provide real-time monitoring and analysis.

### Key Dashboard Panels

1. **Total Trips**

   * Shows the total number of traffic events processed.

2. **Average Travel Time**

   * Shows the overall average journey duration.

3. **Weather-Enriched Events**

   * Shows the number of traffic events successfully enriched with live weather information.

4. **Average Speed**

   * Displays the average vehicle speed.

5. **Traffic Density Distribution**

   * Shows the distribution of traffic events across different congestion levels.

6. **Average Travel Time by Traffic Density**

   * Compares journey duration across traffic density levels.

7. **Weather Impact on Travel Time**

   * Compares average travel time across different historical weather conditions.

8. **Average Speed Over Time**

   * Tracks changes in average speed over the streaming period.

9. **Live Temperature & Precipitation**

   * Displays available live weather conditions.

10. **Top 10 Routes by Average Travel Time**

    * Identifies routes with higher average travel time.

11. **Travel Time vs Distance**

    * Analyses the relationship between trip distance and travel time.

12. **Live Traffic & Weather Events**

    * Displays recent event-level traffic and weather information.

---

## 10. Business Insights

The dashboard provides several business and operational insights.

### Traffic Congestion

Higher traffic density is associated with increased travel time. The dashboard allows users to compare travel-time performance across Low, Medium, High and Very High traffic conditions.

### Weather Conditions

Travel time varies across different weather conditions. The dashboard enables comparison of traffic performance under Clear, Rain, Fog and other recorded conditions.

These relationships are observational and do not by themselves establish that weather or congestion directly causes the observed travel-time differences.

### Route Performance

The route-level analysis helps identify routes with relatively high average travel time and can support route monitoring and operational planning.

### Real-Time Monitoring

The Kafka-based architecture allows incoming traffic and weather events to be processed continuously rather than waiting for a batch-processing cycle.

### Operational Decision-Making

The dashboard can support:

* Fleet monitoring
* Route planning
* Traffic management
* Logistics operations
* Congestion monitoring
* Weather-related operational planning

---

## 11. How to Run the Project

### Step 1 — Activate Virtual Environment

```bash
source venv/bin/activate
```

### Step 2 — Start Kafka

Ensure the Kafka Docker container is running and available at:

```text
localhost:9092
```

### Step 3 — Start Traffic Producer

```bash
python traffic_producer.py
```

### Step 4 — Start Weather Producer

In another terminal:

```bash
python weather_producer.py
```

### Step 5 — Start Consumer

In another terminal:

```bash
python consumer.py
```

The consumer will connect to Kafka, process incoming events and store the processed data in MySQL.

### Step 6 — Open Grafana

Connect Grafana to the MySQL database:

```text
Database: traffic_sda
Table: traffic_weather_events
```

The dashboard can then be used to monitor and analyse the streaming data.

---

## 12. Requirements

Install the required Python packages using:

```bash
pip install -r requirements.txt
```

The project uses libraries for:

* Kafka streaming
* MySQL connectivity
* Environment variable management
* Data processing
* API-based weather data retrieval

---

## 13. Security

Sensitive credentials should not be uploaded to GitHub.

The following files/folders should remain private:

```text
.env
venv/
```

The `.env` file should contain local credentials such as the MySQL password and should **not** be committed to the repository.

---

## 14. Project Outcome

This project demonstrates an end-to-end **real-time streaming analytics solution** using:

**Python → Apache Kafka → Consumer → Weather Enrichment → MySQL → Grafana**

The solution provides continuous processing of traffic and weather events and converts streaming data into actionable visual insights through an interactive dashboard.
