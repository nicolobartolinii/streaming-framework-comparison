from confluent_kafka import Consumer, KafkaError
import json
import logging
import time

logger = logging.getLogger(__name__)


class KafkaStreamsConsumer:
    def __init__(self, bootstrap_servers='localhost:9092', topic='input-topic', group_id='kafka-streams-group'):
        self.consumer = Consumer({
            'bootstrap.servers': bootstrap_servers,
            'group.id': group_id,
            'auto.offset.reset': 'earliest'
        })
        self.topic = topic
        self.is_running = False
        self.processed_count = 0
        self.total_temperature = 0
        self.last_message_time = None
        self.performance_metrics = None

    def start(self, performance_metrics):
        self.performance_metrics = performance_metrics
        self.consumer.subscribe([self.topic])
        self.is_running = True

        logger.info(f"Starting Kafka Streams consumer, listening on topic: {self.topic}")

        while self.is_running:
            msg = self.consumer.poll(1.0)

            if msg is None:
                if self.performance_metrics.producer_finished.is_set() and time.time() - self.last_message_time > 5:
                    logger.info("Producer finished and no messages received for 5 seconds. Stopping consumer.")
                    self.stop()
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    logger.info('Reached end of partition')
                else:
                    logger.error(f'Error: {msg.error()}')
            else:
                try:
                    start_time = time.time()
                    value = json.loads(msg.value().decode('utf-8'))
                    self.process_message(value)
                    self.performance_metrics.record_latency(start_time)
                    self.last_message_time = time.time()
                except json.JSONDecodeError:
                    logger.error(f'Failed to decode message: {msg.value()}')

        logger.info(f"Kafka Streams consumer stopped. Processed {self.processed_count} messages.")
        if self.processed_count > 0:
            average_temperature = self.total_temperature / self.processed_count
            logger.info(f"Average temperature: {average_temperature:.2f}")

    def process_message(self, message):
        self.processed_count += 1
        temperature = message.get('Temperature')
        batt_level = message.get('Batt_level')

        if temperature is not None:
            self.total_temperature += temperature
            logger.debug(
                f'Processed message {self.processed_count}: Temperature = {temperature}, Battery Level = {batt_level}')

        if self.processed_count % 100 == 0:
            logger.info(f'Processed {self.processed_count} messages')

        # Simula un carico di lavoro aggiungendo un piccolo ritardo
        time.sleep(0.0001)  # 0.1 millisecondo di ritardo

    def stop(self):
        self.is_running = False
        self.consumer.close()
        logger.info("Kafka Streams consumer stopped")