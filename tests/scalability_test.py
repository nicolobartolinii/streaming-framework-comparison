import argparse
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from producers.PPGProducer import PPGProducer
from producers.ACCProducer import ACCProducer
from producers.TempBattProducer import TempBattProducer
from consumers.kafka_streams_consumer import KafkaStreamsConsumer
from consumers.spark_streaming_consumer import SparkStreamingConsumer
from utils.performance_metrics import PerformanceMetrics


def run_producer(producer_class, bootstrap_servers, topic, csv_file, frequency, num_devices, duration):
    producer = producer_class(bootstrap_servers, topic, csv_file, frequency, num_devices)
    producer.produce(duration)


def run_consumer(consumer_class, bootstrap_servers, topics, duration):
    consumer = consumer_class(bootstrap_servers, topics)
    consumer.consume(duration)


def run_scalability_test(max_devices, step, duration, consumer_type):
    bootstrap_servers = 'localhost:9092'
    metrics = PerformanceMetrics(bootstrap_servers)

    results = []

    for num_devices in range(step, max_devices + step, step):
        print(f"Testing with {num_devices} devices...")

        # Start producers
        with ThreadPoolExecutor(max_workers=3) as executor:
            executor.submit(run_producer, PPGProducer, bootstrap_servers, 'ppg-topic', 'data/ppg.csv', 100, num_devices,
                            duration)
            executor.submit(run_producer, ACCProducer, bootstrap_servers, 'acc-topic', 'data/acc.csv', 100, num_devices,
                            duration)
            executor.submit(run_producer, TempBattProducer, bootstrap_servers, 'temp-batt-topic', 'data/temp_batt.csv',
                            10, num_devices, duration)

        # Start consumer
        consumer_class = KafkaStreamsConsumer if consumer_type == 'kafka-streams' else SparkStreamingConsumer
        consumer_thread = threading.Thread(target=run_consumer, args=(
        consumer_class, bootstrap_servers, ['ppg-topic', 'acc-topic', 'temp-batt-topic'], duration))
        consumer_thread.start()

        # Wait for test duration
        time.sleep(duration)

        # Stop consumer
        consumer_thread.join()

        # Collect metrics
        test_results = metrics.calculate_results()
        test_results['num_devices'] = num_devices
        results.append(test_results)

        # Clear topics for next test
        metrics.clear_topics(['ppg-topic', 'acc-topic', 'temp-batt-topic'])

        print(f"Test with {num_devices} devices completed.")

    return results


def analyze_results(results):
    print("\nScalability Test Results:")
    print("-------------------------")
    for result in results:
        print(f"Devices: {result['num_devices']}")
        print(f"Throughput: {result['throughput']:.2f} messages/second")
        print(f"Average Latency: {result['avg_latency']:.2f} ms")
        print(f"CPU Usage: {result['cpu_usage']:.2f}%")
        print(f"Memory Usage: {result['memory_usage']:.2f} MB")
        print("-------------------------")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Run scalability tests')
    parser.add_argument('--max-devices', type=int, default=100, help='Maximum number of devices to test')
    parser.add_argument('--step', type=int, default=10, help='Step size for increasing devices')
    parser.add_argument('--duration', type=int, default=300, help='Duration of each test in seconds')
    parser.add_argument('--consumer', choices=['kafka-streams', 'spark'], required=True, help='Type of consumer to use')
    args = parser.parse_args()

    results = run_scalability_test(args.max_devices, args.step, args.duration, args.consumer)
    analyze_results(results)