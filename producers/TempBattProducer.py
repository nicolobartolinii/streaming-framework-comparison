import csv
import json
import time
from kafka import KafkaProducer
from datetime import datetime
import itertools


class TempBattProducer:
    def __init__(self, bootstrap_servers, topic, csv_file, frequency, num_devices):
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        self.topic = topic
        self.csv_file = csv_file
        self.frequency = frequency
        self.num_devices = num_devices
        self.interval = 1.0 / frequency

    def produce(self, stop_event):
        with open(self.csv_file, 'r') as file:
            csv_reader = csv.DictReader(file)
            csv_cycle = itertools.cycle(csv_reader)

            while not stop_event.is_set():
                start_time = time.time()

                for device_id in range(self.num_devices):
                    row = next(csv_cycle)
                    message = {
                        'MAC_Addr': f"device_{device_id}_{row['MAC_Addr']}",
                        'Timestamp': row['Timestamp'],
                        'Temperature': float(row['Temperature']),
                        'Batt_level': float(row['Batt_level']),
                        'Batt_status': row['Batt_status'],
                        'Timestamp_produced': time.time()
                    }
                    self.producer.send(self.topic, message)

                elapsed_time = time.time() - start_time
                sleep_time = self.interval - elapsed_time
                if sleep_time > 0:
                    time.sleep(sleep_time)

    def close(self):
        self.producer.close()