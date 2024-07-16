import unittest
from unittest.mock import MagicMock, patch
from utils.performance_metrics import PerformanceMetrics

class TestPerformanceMetrics(unittest.TestCase):

    def setUp(self):
        self.metrics = PerformanceMetrics('localhost:9092')

    def test_start_stop_measurement(self):
        self.metrics.start_measurement()
        self.assertIsNotNone(self.metrics.start_time)
        self.metrics.stop_measurement()
        self.assertIsNotNone(self.metrics.end_time)

    @patch('utils.performance_metrics.KafkaAdminClient')
    def test_collect_producer_metrics(self, mock_admin_client):
        mock_admin = MagicMock()
        mock_admin_client.return_value = mock_admin
        mock_admin.describe_client_metrics.return_value = {
            'batch-size-avg': 100,
            'compression-rate-avg': 0.5,
            'request-latency-avg': 10,
            'request-rate': 1000,
            'response-rate': 950,
            'record-send-rate': 5000
        }

        self.metrics.collect_producer_metrics('test-producer')
        self.assertIn('test-producer', self.metrics.producer_metrics)
        self.assertEqual(self.metrics.producer_metrics['test-producer']['batch_size_avg'], 100)

    @patch('utils.performance_metrics.KafkaAdminClient')
    def test_collect_consumer_metrics(self, mock_admin_client):
        mock_admin = MagicMock()
        mock_admin_client.return_value = mock_admin
        mock_admin.describe_client_metrics.return_value = {
            'fetch-rate': 500,
            'fetch-size-avg': 1024,
            'fetch-throttle-time-avg': 5,
            'records-consumed-rate': 4500,
            'records-lag-max': 100
        }

        self.metrics.collect_consumer_metrics('test-consumer')
        self.assertEqual(self.metrics.consumer_metrics['fetch_rate'], 500)

    @patch('utils.performance_metrics.KafkaConsumer')
    def test_calculate_data_size(self, mock_kafka_consumer):
        mock_consumer = MagicMock()
        mock_kafka_consumer.return_value = mock_consumer
        mock_consumer.end_offsets.return_value = {0: 1000}

        size = self.metrics.calculate_data_size(['test-topic'], 1)
        self.assertEqual(size, 1024000)  # 1000 messages * 1024 bytes

if __name__ == '__main__':
    unittest.main()