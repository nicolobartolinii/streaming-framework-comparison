import time
import statistics
import logging
import threading

logger = logging.getLogger(__name__)


class PerformanceMetrics:
    def __init__(self):
        self.start_time = None
        self.end_time = None
        self.latencies = []
        self.total_messages = 0
        self.processed_messages = 0
        self.producer_finished = threading.Event()
        self.lock = threading.Lock()

    def start_measurement(self):
        self.start_time = time.time()
        logger.info("Started performance measurement")

    def stop_measurement(self):
        self.end_time = time.time()
        logger.info("Stopped performance measurement")

    def record_latency(self, start_time):
        latency = time.time() - start_time
        with self.lock:
            self.latencies.append(latency)
            self.processed_messages += 1
        logger.debug(f"Recorded latency: {latency}")

    def set_total_messages(self, total):
        self.total_messages = total
        logger.info(f"Set total expected messages: {total}")

    def calculate(self):
        if not self.start_time or not self.end_time:
            logger.error("Measurement not started or stopped properly")
            return None

        total_time = self.end_time - self.start_time

        if self.latencies:
            avg_latency = statistics.mean(self.latencies)
            max_latency = max(self.latencies)
            min_latency = min(self.latencies)
            latency_stats = {
                'average': avg_latency,
                'max': max_latency,
                'min': min_latency
            }
        else:
            latency_stats = None
            logger.warning("No latency data recorded")

        if self.total_messages > 0:
            data_loss = (self.total_messages - self.processed_messages) / self.total_messages
        else:
            data_loss = None
            logger.warning("Total messages not set, unable to calculate data loss")

        throughput = self.processed_messages / total_time if total_time > 0 else 0

        results = {
            'total_time': total_time,
            'latency': latency_stats,
            'data_loss': data_loss,
            'throughput': throughput,
            'processed_messages': self.processed_messages,
            'total_messages': self.total_messages
        }

        logger.info(f"Performance metrics calculated: {results}")
        return results

    def reset(self):
        self.__init__()
        logger.info("Reset performance metrics")