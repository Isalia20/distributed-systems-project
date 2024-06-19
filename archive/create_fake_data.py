import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random

def generate_random_timestamp(start, end):
    return start + timedelta(seconds=random.randint(0, int((end - start).total_seconds())))

def generate_station_id():
    return f"ST{random.randint(1, 9999):04d}"


def generate_sensor_data():
    return round(np.random.uniform(-20.0, 50.0), 3)

def main():
    num_records = 10000
    start_date = datetime(2024, 1, 1)
    end_date = datetime(2024, 12, 31)

    data = {
        "ts": [generate_random_timestamp(start_date, end_date).isoformat() for _ in range(num_records)],
        "station_id": [generate_station_id() for _ in range(num_records)],
        "temperature": [generate_sensor_data() for _ in range(num_records)],
        # "sensor1": [generate_sensor_data() for _ in range(num_records)],
        # "sensor2": [generate_sensor_data() for _ in range(num_records)],
        # "sensor3": [generate_sensor_data() for _ in range(num_records)]
    }

    df = pd.DataFrame(data)

    df.to_csv("fake_dataset.csv", index=False)

    print(df.head())


if __name__ == "__main__":
    main()
