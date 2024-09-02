import csv
import json
import time
from kafka import KafkaProducer
from datetime import datetime
import itertools


class PPGProducer:
    """
    This class is responsible for producing PPG data to a Kafka topic from a specified CSV file.

    For specific comments on the code, please refer to the ACCProducer class.
    """
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
                        'Timestamp_CSV': row['Timestamp'],
                        'PPG1': float(row['PPG1']),
                        'PPG2': float(row['PPG2']),
                        'PPG3': float(row['PPG3']),
                        'PPG4': float(row['PPG4']),
                        'PPG5': float(row['PPG5']),
                        'PPG6': float(row['PPG6']),
                        'Timestamp_produced': time.time()
                    }
                    self.producer.send(self.topic, message)

                elapsed_time = time.time() - start_time
                sleep_time = self.interval - elapsed_time
                if sleep_time > 0:
                    time.sleep(sleep_time)

    def close(self):
        self.producer.close()