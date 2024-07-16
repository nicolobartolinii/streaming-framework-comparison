import unittest
from unittest.mock import MagicMock, patch
from producers.PPGProducer import PPGProducer
from producers.ACCProducer import ACCProducer
from producers.TempBattProducer import TempBattProducer

class TestProducers(unittest.TestCase):

    @patch('producers.ppg_producer.KafkaProducer')
    def test_ppg_producer(self, mock_kafka_producer):
        mock_producer = MagicMock()
        mock_kafka_producer.return_value = mock_producer

        producer = PPGProducer('localhost:9092', 'ppg-topic', 'test_data.csv', 100, 1)
        producer.produce(MagicMock())

        self.assertTrue(mock_producer.send.called)
        mock_producer.send.assert_called_with('ppg-topic', unittest.mock.ANY)

    @patch('producers.acc_producer.KafkaProducer')
    def test_acc_producer(self, mock_kafka_producer):
        mock_producer = MagicMock()
        mock_kafka_producer.return_value = mock_producer

        producer = ACCProducer('localhost:9092', 'acc-topic', 'test_data.csv', 100, 1)
        producer.produce(MagicMock())

        self.assertTrue(mock_producer.send.called)
        mock_producer.send.assert_called_with('acc-topic', unittest.mock.ANY)

    @patch('producers.temp_batt_producer.KafkaProducer')
    def test_temp_batt_producer(self, mock_kafka_producer):
        mock_producer = MagicMock()
        mock_kafka_producer.return_value = mock_producer

        producer = TempBattProducer('localhost:9092', 'temp-batt-topic', 'test_data.csv', 10, 1)
        producer.produce(MagicMock())

        self.assertTrue(mock_producer.send.called)
        mock_producer.send.assert_called_with('temp-batt-topic', unittest.mock.ANY)

if __name__ == '__main__':
    unittest.main()