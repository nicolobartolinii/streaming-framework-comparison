import numpy as np
from confluent_kafka import Consumer, KafkaError
import json
import time
from prometheus_client import start_http_server, Counter, Gauge, Histogram
from brainflow.data_filter import DataFilter

# PROMETHEUS METRICS
# Counter for the number of messages consumed by the consumer
MESSAGES_CONSUMED = Counter('kafka_consumer_messages_consumed_total', 'Total number of messages consumed', ['topic'])
# Histogram for the latency between the message produced and consumed
LATENCY_METRIC = Histogram('message_latency_seconds', 'Latency between message produced and consumed', ['topic'])
# Gauge for the heart rate from the Fitband
HR_GAUGE = Gauge('fitband_heart_rate', 'Heart Rate from Fitband', ['device_id'])
# Gauge for the oxygen level from the Fitband
SPO2_GAUGE = Gauge('fitband_oxygen_level', 'Oxygen Level from Fitband', ['device_id'])


class KafkaStreamsConsumer:
    """
    This class is responsible for consuming data from Kafka topics and processing it. It uses Kafka Streams.
    """
    def __init__(self, bootstrap_servers, topics, group_id):
        """
        Initializes the consumer with the specified parameters.

        :param bootstrap_servers: the bootstrap servers for the Kafka cluster
        :param topics: the list of topics to consume from
        :param group_id: the group ID for the consumer
        """
        self.consumer = Consumer({
            'bootstrap.servers': bootstrap_servers,
            'group.id': group_id,
            'auto.offset.reset': 'earliest'
        })
        self.consumer.subscribe(topics)
        self.running = True

        self.ppg3_accumulated = []
        self.ppg4_accumulated = []
        self.ppg5_accumulated = []
        self.ppg6_accumulated = []
        self.accumulation_threshold = 1024

        start_http_server(8001)

    def consume(self, stop_event):
        """
        This method consumes data from the Kafka topics and processes it. It runs until the stop_event is set.

        :param stop_event: the stop event to stop the consumer
        :return: None
        """
        try:
            while not stop_event.is_set():  # Run until the stop_event is set
                msg = self.consumer.poll(1.0)  # Poll for a message
                if msg is None:  # If there is no message
                    continue  # Continue to the next iteration
                if msg.error():  # If there is an error
                    if msg.error().code() == KafkaError._PARTITION_EOF:  # If the error is a partition EOF
                        print('Reached end of partition')  # Print a message
                    else:  # If the error is not a partition EOF
                        print('Error: {}'.format(msg.error()))  # Print the error
                else:  # If there is no error, there is a message
                    MESSAGES_CONSUMED.labels(topic=msg.topic()).inc()  # Increment the message consumed counter

                    message = json.loads(msg.value())  # Parse the message as JSON

                    produced_timestamp = message['Timestamp_produced']  # Get the produced timestamp
                    latency = calculate_latency(produced_timestamp)  # Calculate the latency

                    print(f'Latenza: {latency} secondi')  # Print the latency (for information purposes)

                    LATENCY_METRIC.labels(topic=msg.topic()).observe(latency)  # Observe the latency (for Prometheus)

                    if all(key in message for key in ['PPG3', 'PPG4', 'PPG5', 'PPG6']):  # If all PPG data is present
                        self.ppg3_accumulated.append(message['PPG3'])  # Append the PPG3 data to the accumulator
                        self.ppg4_accumulated.append(message['PPG4'])  # Append the PPG4 data to the accumulator
                        self.ppg5_accumulated.append(message['PPG5'])  # Append the PPG5 data to the accumulator
                        self.ppg6_accumulated.append(message['PPG6'])  # Append the PPG6 data to the accumulator

                        if len(self.ppg3_accumulated) >= self.accumulation_threshold:  # If the accumulator is full
                            self.process_ppg_data(device_id=message['MAC_Addr'])  # Process the PPG data
                    else:  # If not all PPG data is present
                        print('Received message: {}'.format(message))  # Print the message (for information purposes)
        finally:
            self.consumer.close()  # Close the consumer

    def process_ppg_data(self, device_id):
        """
        This method processes PPG data received from a Kafka topic using BrainFlow.

        :param device_id: the ID of the device
        :return: None
        """
        ppg_ir = np.array(self.ppg3_accumulated[-self.accumulation_threshold:])  # Convert the PPG3 accumulator to a numpy array
        ppg_red = np.array(self.ppg5_accumulated[-self.accumulation_threshold:])  # Convert the PPG5 accumulator to a numpy array

        try:  # Try to calculate SpO2 and HR using BrainFlow
            spo2 = DataFilter.get_oxygen_level(ppg_ir, ppg_red, 25, coef3=130.6898759)  # Calculate SpO2
            hr = DataFilter.get_heart_rate(ppg_ir, ppg_red, 25, self.accumulation_threshold)  # Calculate HR

            HR_GAUGE.labels(device_id).set(hr)  # Set the HR gauge
            SPO2_GAUGE.labels(device_id).set(spo2)  # Set the SpO2 gauge

            print(f"HR: {hr} - SpO2: {spo2}")  # Print the HR and SpO2 (for information purposes)
        except Exception as e:  # If an exception occurs
            print(f"Error processing PPG data: {e}")  # Print the error (for information purposes)

    def stop(self):
        """
        Stops the consumer.

        :return: None
        """
        self.running = False  # Set the running flag to False to stop the consumer


def calculate_latency(produced_timestamp):
    """
    Calculates the latency between the message produced and consumed.

    :param produced_timestamp: the timestamp of the message produced
    :return: the latency in seconds
    """
    consumed_timestamp = time.time()  # Get the current time
    return consumed_timestamp - produced_timestamp  # Calculate the latency
