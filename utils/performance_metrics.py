# Nome file: performance_metrics.py
# Posizione: /fitband_streaming_project/utils/performance_metrics.py

import time
from kafka import KafkaAdminClient, KafkaConsumer
from kafka.admin import ConfigResource, ConfigResourceType
from kafka.errors import KafkaError
import json
from kafka import TopicPartition
import psutil


class PerformanceMetrics:
    def __init__(self, bootstrap_servers):
        self.bootstrap_servers = bootstrap_servers
        self.admin_client = KafkaAdminClient(bootstrap_servers=bootstrap_servers)
        self.start_time = None
        self.end_time = None
        self.cpu_usage = []
        self.memory_usage = []

    def start_measurement(self):
        self.start_time = time.time()

    def stop_measurement(self):
        self.end_time = time.time()

    def collect_system_metrics(self):
        self.cpu_usage.append(psutil.cpu_percent())
        self.memory_usage.append(psutil.virtual_memory().percent)

    def calculate_data_size(self, topics, duration_seconds):
        start_time = time.time()
        end_time = start_time + duration_seconds

        start_offsets = self.get_current_offsets(topics)
        time.sleep(duration_seconds)
        end_offsets = self.get_current_offsets(topics)

        total_messages = sum(end_offsets[topic] - start_offsets[topic] for topic in topics)

        # Assumiamo una dimensione media del messaggio di 1KB
        estimated_size_bytes = total_messages * 1024

        return estimated_size_bytes

    def get_current_offsets(self, topics):
        consumer = KafkaConsumer(
            bootstrap_servers=self.bootstrap_servers,
            auto_offset_reset='latest',
            consumer_timeout_ms=1000
        )
        offsets = {}
        for topic in topics:
            partitions = consumer.partitions_for_topic(topic)
            if partitions:
                topic_partitions = [TopicPartition(topic, p) for p in partitions]
                end_offsets = consumer.end_offsets(topic_partitions)
                topic_offset = sum(end_offsets.values())
                offsets[topic] = topic_offset
            else:
                offsets[topic] = 0
        consumer.close()
        return offsets

    def print_data_size(self, topics, duration_seconds):
        data_size = self.calculate_data_size(topics, duration_seconds)
        print(f"Estimated data size for {duration_seconds} seconds of streaming: {data_size / (1024 * 1024):.2f} MB")

    def calculate_results(self):
        if not self.start_time or not self.end_time:
            raise ValueError("Measurement not started or stopped properly")

        duration = self.end_time - self.start_time
        avg_cpu_usage = sum(self.cpu_usage) / len(self.cpu_usage) if self.cpu_usage else 0
        avg_memory_usage = sum(self.memory_usage) / len(self.memory_usage) if self.memory_usage else 0

        results = {
            'duration': duration,
            'avg_cpu_usage': avg_cpu_usage,
            'avg_memory_usage': avg_memory_usage,
        }

        return results

    def print_results(self):
        results = self.calculate_results()
        print(json.dumps(results, indent=2))


if __name__ == "__main__":
    metrics = PerformanceMetrics("localhost:9092")
    metrics.start_measurement()
    # Simulate some activity
    for _ in range(10):
        metrics.collect_system_metrics()
        time.sleep(1)
    metrics.stop_measurement()
    metrics.print_results()