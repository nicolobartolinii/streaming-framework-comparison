from confluent_kafka import Consumer, KafkaError
import json
import time
from prometheus_client import start_http_server, Counter, Gauge

MESSAGES_CONSUMED = Counter('kafka_consumer_messages_consumed_total', 'Total number of messages consumed', ['topic'])

class KafkaStreamsConsumer:
    def __init__(self, bootstrap_servers, topics, group_id):
        self.consumer = Consumer({
            'bootstrap.servers': bootstrap_servers,
            'group.id': group_id,
            'auto.offset.reset': 'earliest'
        })
        self.consumer.subscribe(topics)
        self.running = True

        self.temperature_gauge = Gauge('fitband_temperature', 'Temperature of the Fitband', ['device_id'])

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
                    if 'Temperature' in message:
                        self.process_message(message)
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

    def stop(self):
        self.running = False
