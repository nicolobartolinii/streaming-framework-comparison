# main.py

import argparse
import threading
import time
from producers.PPGProducer import PPGProducer
from producers.ACCProducer import ACCProducer
from producers.TempBattProducer import TempBattProducer
from consumers.kafka_streams_consumer import KafkaStreamsConsumer
from consumers.spark_streaming_consumer import SparkStreamingConsumer  # Ensure this path is correct
from utils.performance_metrics import PerformanceMetrics

def run_producer(producer, stop_event):
    producer.produce(stop_event)

def run_spark_consumer(consumer, stop_event):
    consumer.run()  # Ensure this method starts the stream and keeps it running

def main():
    parser = argparse.ArgumentParser(description='Run Fitband data streaming test')
    parser.add_argument('--consumer', choices=['kafka-streams', 'spark'], required=True)
    parser.add_argument('--duration', type=int, default=600)
    parser.add_argument('--ppg-rate', type=int, default=100)
    parser.add_argument('--acc-rate', type=int, default=100)
    parser.add_argument('--temp-batt-rate', type=int, default=10)
    parser.add_argument('--num-devices', type=int, default=1)
    args = parser.parse_args()

    bootstrap_servers = 'localhost:29092'
    stop_event = threading.Event()

    metrics = PerformanceMetrics(bootstrap_servers)

    ppg_producer = PPGProducer(bootstrap_servers, 'ppg-topic', 'data/ppg.csv', args.ppg_rate, args.num_devices)
    acc_producer = ACCProducer(bootstrap_servers, 'acc-topic', 'data/acc.csv', args.acc_rate, args.num_devices)
    temp_batt_producer = TempBattProducer(bootstrap_servers, 'temp-batt-topic', 'data/temp_batt.csv', args.temp_batt_rate, args.num_devices)

    if args.consumer == 'kafka-streams':
        consumer = KafkaStreamsConsumer(bootstrap_servers, ['ppg-topic', 'acc-topic', 'temp-batt-topic'], 'fitband-consumer-group')
        consumer_thread = threading.Thread(target=consumer.consume, args=(stop_event,))
    else:
        consumer = SparkStreamingConsumer(bootstrap_servers, ['ppg-topic', 'acc-topic', 'temp-batt-topic'])
        consumer_thread = threading.Thread(target=run_spark_consumer, args=(consumer, stop_event))

    metrics.start_measurement()

    producer_threads = [
        threading.Thread(target=run_producer, args=(ppg_producer, stop_event)),
        threading.Thread(target=run_producer, args=(acc_producer, stop_event)),
        threading.Thread(target=run_producer, args=(temp_batt_producer, stop_event))
    ]

    for thread in producer_threads:
        thread.start()

    consumer_thread.start()

    time.sleep(args.duration)

    stop_event.set()

    for thread in producer_threads:
        thread.join()
    consumer_thread.join()

    metrics.stop_measurement()
    metrics.print_results()

if __name__ == "__main__":
    main()