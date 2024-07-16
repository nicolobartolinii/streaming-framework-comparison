from confluent_kafka import Consumer, KafkaError
import json
import time


class KafkaStreamsConsumer:
    def __init__(self, bootstrap_servers, topics, group_id):
        self.consumer = Consumer({
            'bootstrap.servers': bootstrap_servers,
            'group.id': group_id,
            'auto.offset.reset': 'earliest'
        })
        self.consumer.subscribe(topics)
        self.running = True

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
                    print('Received message: {}'.format(json.loads(msg.value())))
        finally:
            self.consumer.close()

    def stop(self):
        self.running = False
