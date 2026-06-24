import requests
from datetime import datetime, timedelta, timezone
import time


url = "https://earthquake.usgs.gov/fdsnws/event/1/query"

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

last_time = datetime.now(timezone.utc) - timedelta(hours=1)

while True:
    quakes = get_earthquakes(last_time)
    print(f"Получено {len(quakes)} событий")

    last_time = datetime.now(timezone.utc)
    time.sleep(60)


