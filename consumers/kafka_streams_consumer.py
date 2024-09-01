from venv import logger

import numpy as np
from confluent_kafka import Consumer, KafkaError
import json
import time
from prometheus_client import start_http_server, Counter, Gauge, Histogram
from brainflow.data_filter import DataFilter

MESSAGES_CONSUMED = Counter('kafka_consumer_messages_consumed_total', 'Total number of messages consumed', ['topic'])
LATENCY_METRIC = Histogram('message_latency_seconds', 'Latency between message produced and consumed', ['topic'])
HR_GAUGE = Gauge('fitband_heart_rate', 'Heart Rate from Fitband', ['device_id'])
SPO2_GAUGE = Gauge('fitband_oxygen_level', 'Oxygen Level from Fitband', ['device_id'])



class KafkaStreamsConsumer:
    def __init__(self, bootstrap_servers, topics, group_id):
        logger.error(f"Initializing KafkaStreamsConsumer with bootstrap servers: {bootstrap_servers}")
        self.consumer = Consumer({
            'bootstrap.servers': bootstrap_servers,
            'group.id': group_id,
            'auto.offset.reset': 'earliest'
        })
        self.consumer.subscribe(topics)
        self.running = True

        self.temperature_gauge = Gauge('fitband_temperature', 'Temperature of the Fitband', ['device_id'])

        self.ppg3_accumulated = []
        self.ppg4_accumulated = []
        self.ppg5_accumulated = []
        self.ppg6_accumulated = []
        self.accumulation_threshold = 2048

        start_http_server(8001)

    def consume(self, stop_event):  # Aggiungi stop_event come parametro
        try:
            while not stop_event.is_set():  # Usa stop_event invece di self.running
                msg = self.consumer.poll(1.0)
                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        print('Reached end of partition')
                    else:
                        print('Error: {}'.format(msg.error()))
                else:
                    MESSAGES_CONSUMED.labels(topic=msg.topic()).inc()

                    message = json.loads(msg.value())

                    produced_timestamp = message['Timestamp_produced']
                    latency = calculate_latency(produced_timestamp)

                    print(f'Latenza: {latency} secondi')

                    LATENCY_METRIC.labels(topic=msg.topic()).observe(latency)

                    if 'Temperature' in message:
                        self.process_message(message)
                    elif all(key in message for key in ['PPG3', 'PPG4', 'PPG5', 'PPG6']):
                        self.ppg3_accumulated.append(message['PPG3'])
                        self.ppg4_accumulated.append(message['PPG4'])
                        self.ppg5_accumulated.append(message['PPG5'])
                        self.ppg6_accumulated.append(message['PPG6'])

                        if len(self.ppg3_accumulated) >= self.accumulation_threshold:
                            self.process_ppg_data(device_id=message['MAC_Addr'])
                    else:
                        print('Received message: {}'.format(message))
        finally:
            self.consumer.close()

    def process_message(self, message):
        device_id = message['MAC_Addr']
        temperature = float(message['Temperature'])

        doubled_temperature = temperature * 2

        self.temperature_gauge.labels(device_id).set(doubled_temperature)

        print(f"Device {device_id} - Original temperature: {temperature} - Doubled temperature: {doubled_temperature}")

    def process_ppg_data(self, device_id):
        # Converti i dati accumulati in numpy array
        ppg_ir = np.array(self.ppg3_accumulated[-self.accumulation_threshold:])
        ppg_red = np.array(self.ppg5_accumulated[-self.accumulation_threshold:])

        # Calcolare SpO2 e HR usando BrainFlow
        try:
            spo2 = DataFilter.get_oxygen_level(ppg_ir, ppg_red, 100, coef3=130.6898759)
            hr = DataFilter.get_heart_rate(ppg_ir, ppg_red, 100, self.accumulation_threshold)

            # Aggiornare le metriche Prometheus
            HR_GAUGE.labels(device_id).set(hr)
            SPO2_GAUGE.labels(device_id).set(spo2)

            print(f"HR: {hr} - SpO2: {spo2}")
        except Exception as e:
            print(f"Error processing PPG data: {e}")

    def stop(self):
        self.running = False


def calculate_latency(produced_timestamp):
    consumed_timestamp = time.time()
    return consumed_timestamp - produced_timestamp
