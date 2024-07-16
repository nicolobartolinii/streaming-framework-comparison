import unittest
from unittest.mock import MagicMock, patch
from consumers.kafka_streams_consumer import KafkaStreamsConsumer
from consumers.spark_streaming_consumer import SparkStreamingConsumer

class TestConsumers(unittest.TestCase):

    @patch('consumers.kafka_streams_consumer.KafkaConsumer')
    def test_kafka_streams_consumer(self, mock_kafka_consumer):
        mock_consumer = MagicMock()
        mock_kafka_consumer.return_value = mock_consumer

        consumer = KafkaStreamsConsumer('localhost:9092', ['test-topic'], 'test-group')
        consumer.consume(MagicMock())

        self.assertTrue(mock_consumer.subscribe.called)
        mock_consumer.subscribe.assert_called_with(['test-topic'])

    @patch('consumers.spark_streaming_consumer.SparkSession')
    def test_spark_streaming_consumer(self, mock_spark_session):
        mock_spark = MagicMock()
        mock_spark_session.builder.appName().getOrCreate.return_value = mock_spark

        consumer = SparkStreamingConsumer('localhost:9092', ['test-topic'])
        consumer.consume(MagicMock())

        self.assertTrue(mock_spark.readStream.format.called)
        mock_spark.readStream.format.assert_called_with("kafka")

if __name__ == '__main__':
    unittest.main()