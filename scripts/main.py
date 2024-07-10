import argparse
import logging
import time
import threading
from kafka import KafkaAdminClient
from kafka.admin import NewTopic
from kafka.errors import TopicAlreadyExistsError
from pyspark.sql import SparkSession
from producers.kafka_producer import KafkaProducerWrapper
from producers.pulsar_producer import PulsarProducerWrapper
from consumers.kafka_streams_consumer import KafkaStreamsConsumer
from consumers.spark_streaming_consumer import SparkStreamingConsumer
# from consumers.flink_consumer import FlinkConsumer
from utils.csv_reader import CSVReader
from utils.performance_metrics import PerformanceMetrics

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(name)s - %(message)s')
logger = logging.getLogger(__name__)


def create_spark_session():
    return SparkSession.builder \
        .appName("KafkaSparkStreamingTest") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1") \
        .getOrCreate()


def create_kafka_topic(bootstrap_servers, topic_name):
    admin_client = KafkaAdminClient(bootstrap_servers=bootstrap_servers)
    try:
        admin_client.delete_topics([topic_name])
        logger.info(f"Deleted existing topic: {topic_name}")
    except:
        pass

    try:
        topic = NewTopic(name=topic_name, num_partitions=1, replication_factor=1)
        admin_client.create_topics([topic])
        logger.info(f"Created new topic: {topic_name}")
    except TopicAlreadyExistsError:
        logger.info(f"Topic already exists: {topic_name}")
    finally:
        admin_client.close()


def run_producer(producer, csv_reader, data_rate, metrics):
    try:
        producer.start(csv_reader, data_rate)
    except Exception as e:
        logger.error(f"Error in producer: {e}", exc_info=True)
    finally:
        metrics.producer_finished.set()


def run_consumer(consumer, metrics):
    try:
        consumer.start(metrics)
    except Exception as e:
        logger.error(f"Error in consumer: {e}", exc_info=True)
    finally:
        consumer.stop()


def run_test(producer, consumer, data_rate, topic_name):
    metrics = PerformanceMetrics()
    metrics.start_measurement()

    csv_reader = CSVReader('data/input.csv')
    csv_reader.open()

    total_messages = sum(1 for _ in csv_reader)
    csv_reader.reset()
    metrics.set_total_messages(total_messages)

    producer_thread = threading.Thread(target=run_producer, args=(producer, csv_reader, data_rate, metrics))
    consumer_thread = threading.Thread(target=run_consumer, args=(consumer, metrics))

    producer_thread.start()
    consumer_thread.start()

    producer_thread.join()
    consumer_thread.join()

    csv_reader.close()

    metrics.stop_measurement()
    results = metrics.calculate()
    logger.info(f"Test results: {results}")


def main():
    parser = argparse.ArgumentParser(description='Run stream processing tests')
    parser.add_argument('--producer', choices=['kafka', 'pulsar'], required=True)
    parser.add_argument('--consumer', choices=['kafka-streams', 'spark', 'flink'], required=True)
    parser.add_argument('--rate', type=int, choices=[10, 100], required=True)
    parser.add_argument('--bootstrap-servers', default='localhost:9092')
    args = parser.parse_args()

    topic_name = 'input-topic'
    create_kafka_topic(args.bootstrap_servers, topic_name)

    spark = create_spark_session()

    try:
        if args.producer == 'kafka':
            producer = KafkaProducerWrapper(bootstrap_servers=args.bootstrap_servers, topic=topic_name)
        else:
            producer = PulsarProducerWrapper()

        if args.consumer == 'kafka-streams':
            consumer = KafkaStreamsConsumer(bootstrap_servers=args.bootstrap_servers, topic=topic_name)
        elif args.consumer == 'spark':
            consumer = SparkStreamingConsumer(spark, bootstrap_servers=args.bootstrap_servers, topic=topic_name)
        else:
            # consumer = FlinkConsumer(bootstrap_servers=args.bootstrap_servers, topic=topic_name)
            logger.error("Flink consumer not implemented")

        run_test(producer, consumer, args.rate, topic_name)
    except Exception as e:
        logger.error(f"Unexpected error: {e}", exc_info=True)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()