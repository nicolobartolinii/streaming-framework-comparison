# main.py

import argparse
import threading
import time
from producers.PPGProducer import PPGProducer
from producers.ACCProducer import ACCProducer
from producers.TempBattProducer import TempBattProducer
from consumers.kafka_streams_consumer import KafkaStreamsConsumer
from consumers.spark_streaming_consumer import SparkStreamingConsumer


def run_producer(producer, stop_event):
    """
    This function runs a kafka streams producer and keeps it running until the stop_event is set.

    :param producer: The producer to run.
    :param stop_event: The event to stop the producer.
    :return: None
    """
    producer.produce(stop_event)


def run_spark_consumer(consumer):
    """
    This function runs a spark consumer.
    :param consumer: The consumer to run.
    :return: None
    """
    consumer.run()


def main():
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Run Fitband data streaming test')
    # Kafka Streams or Spark Streaming as the consumer
    parser.add_argument('--consumer', choices=['kafka-streams', 'spark'], required=True)
    # Duration of the test in seconds
    parser.add_argument('--duration', type=int, default=600)
    # Rate of PPG data in Hz
    parser.add_argument('--ppg-rate', type=int, default=100)
    # Rate of ACC data in Hz
    parser.add_argument('--acc-rate', type=int, default=100)
    # Rate of Temperature and Battery data in Hz
    parser.add_argument('--temp-batt-rate', type=int, default=10)
    # Number of devices to simulate
    parser.add_argument('--num-devices', type=int, default=1)
    args = parser.parse_args()

    # Bootstrap servers for Kafka (localhost:29092 for local deployment)
    bootstrap_servers = 'localhost:29092'
    # Create a stop event to stop the producers and consumers
    stop_event = threading.Event()

    # Create the three producers (one for each topic/CSV file)
    # ppg_producer = PPGProducer(bootstrap_servers, 'ppg-topic', 'data/ppg.csv', args.ppg_rate, args.num_devices)
    # acc_producer = ACCProducer(bootstrap_servers, 'acc-topic', 'data/acc.csv', args.acc_rate, args.num_devices)
    # temp_batt_producer = TempBattProducer(bootstrap_servers, 'temp-batt-topic', 'data/temp_batt.csv',
    #                                      args.temp_batt_rate, args.num_devices)

    # Create the consumer based on the command line argument (Kafka Streams or Spark Streaming)
    if args.consumer == 'kafka-streams':
        consumer = KafkaStreamsConsumer(bootstrap_servers, ['ppg-topic', 'acc-topic', 'temp-batt-topic'],
                                        'fitband-consumer-group')
        consumer_thread = threading.Thread(target=consumer.consume, args=(stop_event,))
    else:
        consumer = SparkStreamingConsumer(bootstrap_servers, ['ppg-topic', 'acc-topic', 'temp-batt-topic'])
        consumer_thread = threading.Thread(target=run_spark_consumer, args=(consumer, stop_event))

    # Insert the producers inside a list. Each element in the list is a thread that runs the producer.
    # producer_threads = [
    #     threading.Thread(target=run_producer, args=(ppg_producer, stop_event)),
    #     threading.Thread(target=run_producer, args=(acc_producer, stop_event)),
    #     threading.Thread(target=run_producer, args=(temp_batt_producer, stop_event))
    # ]

    # Start the producers
    # for thread in producer_threads:
    #     thread.start()

    # Start the consumer
    consumer_thread.start()

    # Sleep for the duration of the test
    time.sleep(args.duration)

    # Set the stop event to stop the producers and consumer
    stop_event.set()

    # Join the producers and consumer threads
    # for thread in producer_threads:
    #     thread.join()
    consumer_thread.join()


if __name__ == "__main__":
    main()
