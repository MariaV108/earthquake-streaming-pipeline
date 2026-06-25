import os
import requests
import psycopg2
from datetime import datetime, timedelta, timezone
import time
import json
from confluent_kafka import Producer


KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
TOPIC = "earthquakes"

producer = Producer({"bootstrap.servers": KAFKA_BROKER})

url = "https://earthquake.usgs.gov/fdsnws/event/1/query"


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

pg_conn = connect_postgress()
pg_cur = pg_conn.cursor()


def read_checkpoint():
    pg_cur.execute("SELECT last_time FROM producer_checkpoint WHERE id = 1")
    row = pg_cur.fetchone()
    if row is not None:
        print(f"Checkpoint found, continuing with {row[0]}")
        return row[0]
    else:
        print("There’s no checkpoint, starting from the last hour")
        return datetime.now(timezone.utc) - timedelta(hours=1)



def write_checkpoint(moment):
    pg_cur.execute("""
        INSERT INTO producer_checkpoint (id, last_time)
        VALUES(1, %s)
        ON CONFLICT (id) DO UPDATE SET last_time = EXCLUDED.last_time
    """, (moment,))

def get_earthquakes(start_time):

    params = {
        "format": "geojson",
        "starttime": start_time.isoformat(),
        "minmagnitude": 5
        }

    raw_data = []
    response = requests.get(url, params=params)
    data = response.json()

    for earthquake in data['features']:
        record = {
            'event_id': earthquake['id'],
            'magnitude': earthquake['properties']['mag'],
            'place': earthquake['properties']['place'],
            'event_time': datetime.fromtimestamp(earthquake['properties']['time']/1000, tz=timezone.utc),
            'longitude': earthquake['geometry']['coordinates'][0],
            'latitude': earthquake['geometry']['coordinates'][1],
            'depth': earthquake['geometry']['coordinates'][2],
            'tsunami': earthquake['properties']['tsunami'],
            'significance': earthquake['properties']['sig'],
            'event_type': earthquake['properties']['type']
        }
        raw_data.append(record)

    return raw_data

last_time = read_checkpoint()

while True:
    quakes = get_earthquakes(last_time)
    for quake in quakes:
        producer.produce(topic=TOPIC, value=json.dumps(quake, default=str))
    producer.flush()

    last_time = datetime.now(timezone.utc)
    write_checkpoint(last_time)
    print(f"[{datetime.now(timezone.utc):%H:%M:%S}] {len(quakes)} events sent")

    time.sleep(60)

