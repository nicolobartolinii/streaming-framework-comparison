import pulsar
import json
import time
import logging

logger = logging.getLogger(__name__)

class PulsarProducerWrapper:
    def __init__(self, service_url='pulsar://localhost:6650', topic='input-topic'):
        self.client = pulsar.Client(service_url)
        self.producer = self.client.create_producer(topic)
        self.is_running = False

    def start(self, csv_reader, data_rate):
        self.is_running = True
        interval = 1.0 / data_rate  # Calcola l'intervallo basato sulla frequenza dei dati

        while self.is_running:
            try:
                data = next(csv_reader)
                self.producer.send(json.dumps(data).encode('utf-8'))
                logger.info(f"Sent data to Pulsar: {data}")
                time.sleep(interval)
            except StopIteration:
                logger.info("Finished reading CSV file")
                self.stop()
            except Exception as e:
                logger.error(f"Error sending data to Pulsar: {e}")

    def stop(self):
        self.is_running = False
        self.producer.close()
        self.client.close()
        logger.info("Pulsar producer stopped")