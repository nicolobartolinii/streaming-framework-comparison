from kafka import KafkaProducer
from kafka.errors import KafkaError
import json
import time
import logging

logger = logging.getLogger(__name__)

class KafkaProducerWrapper:
    def __init__(self, bootstrap_servers='localhost:9092', topic='input-topic'):
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            acks='all',
            retries=3,
            max_in_flight_requests_per_connection=1,
            api_version_auto_timeout_ms=30000
        )
        self.topic = topic
        self.is_running = False

    def start(self, csv_reader, data_rate):
        self.is_running = True
        interval = 1.0 / data_rate  # Calcola l'intervallo basato sulla frequenza dei dati

        while self.is_running:
            try:
                data = next(csv_reader)
                future = self.producer.send(self.topic, data)
                future.get(timeout=10)  # Wait for the send to complete
                logger.info(f"Sent data to Kafka: {data}")
                time.sleep(interval)
            except StopIteration:
                logger.info("Finished reading CSV file")
                self.stop()
            except KafkaError as e:
                logger.error(f"Error sending data to Kafka: {e}")
            except Exception as e:
                logger.error(f"Unexpected error: {e}")

    def stop(self):
        self.is_running = False
        self.producer.close()
        logger.info("Kafka producer stopped")