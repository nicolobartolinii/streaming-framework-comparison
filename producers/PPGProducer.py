import csv
import json
import threading
import time
from kafka import KafkaProducer
from datetime import datetime


class PPGProducer:
    def __init__(self, bootstrap_servers, topic, csv_file, frequency, num_devices):
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        self.topic = topic
        self.csv_file = csv_file
        self.frequency = frequency
        self.num_devices = num_devices

    def produce(self, stop_event):
        with open(self.csv_file, 'r') as file:
            csv_reader = csv.DictReader(file)
            start_time = time.time()
            for row in csv_reader:
                if stop_event.is_set():
                    break
                for device_id in range(self.num_devices):
                    message = {
                        'MAC_Addr': f"device_{device_id}_{row['MAC_Addr']}",
                        'Timestamp': datetime.now().isoformat(),
                        'PPG1': float(row['PPG1']),
                        'PPG2': float(row['PPG2']),
                        'PPG3': float(row['PPG3']),
                        'PPG4': float(row['PPG4']),
                        'PPG5': float(row['PPG5']),
                        'PPG6': float(row['PPG6'])
                    }
                    self.producer.send(self.topic, message)

                # Control the sending rate
                elapsed_time = time.time() - start_time
                expected_messages = elapsed_time * self.frequency
                if csv_reader.line_num < expected_messages:
                    time.sleep((expected_messages - csv_reader.line_num) / self.frequency)

    def close(self):
        self.producer.close()


if __name__ == "__main__":
    producer = PPGProducer('localhost:29092', 'ppg-topic', 'data/ppg.csv', 100, 1)
    try:
        producer.produce(threading.Event())
    except KeyboardInterrupt:
        pass
    finally:
        producer.close()