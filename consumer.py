import os
import json
import time
import psycopg2
from confluent_kafka import Consumer


KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
TOPIC = "earthquakes"

def connect_postgress():
    conn = psycopg2.connect(
        host=os.getenv("PG_HOST", "localhost"),
        port=os.getenv("PG_PORT", "5432"),
        dbname=os.getenv("PG_DB", "earthquakes"),
        user=os.getenv("PG_USER", "postgres"),
        password=os.getenv("PG_PASSWORD", "postgres")
    )
    conn.autocommit = True
    return conn

conn = connect_postgress()
cur = conn.cursor()

consumer = Consumer({
    "bootstrap.servers": KAFKA_BROKER,
    "group.id": "earthquake-consumer",
    "auto.offset.reset": "earliest",
})
consumer.subscribe([TOPIC])
print("Consumer has been launched, waiting for a message...")

while True:
    msg = consumer.poll(1.0)
    if msg is None:
        continue
    if msg.error():
        print("Error Kafka:", msg.error())
        continue

    quake = json.loads(msg.value())

    if quake["magnitude"] >= 7:
        quake["alert_level"] = "RED"
    elif quake["magnitude"] >= 6:
        quake["alert_level"] = "ORANGE"
    else:
        quake["alert_level"] = "GREEN"

    try:
        cur.execute("""
                    INSERT INTO earthquakes (
                event_id, magnitude, place, event_time,
                longitude, latitude, depth, tsunami,
                significance, event_type, alert_level
            )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (event_id) DO NOTHING
                    """, (
                        quake["event_id"], quake["magnitude"], quake["place"], quake["event_time"],
                        quake["longitude"], quake["latitude"], quake["depth"], quake["tsunami"],
                        quake["significance"], quake["event_type"], quake["alert_level"]
                        ))
    except psycopg2.OperationalError as e:
        print(f"⚠️  Connection to Postgres lost: {e}. Reconnecting...")
        time.sleep(5)
        conn = connect_postgress()
        cur = conn.cursor()
        continue

    if quake["alert_level"] in ("RED", "ORANGE"):
        print(f"🚨 [{quake['alert_level']}] M{quake['magnitude']} — {quake['place']}")
    else:
        print(f"   saved: M{quake['magnitude']} — {quake['place']}")
