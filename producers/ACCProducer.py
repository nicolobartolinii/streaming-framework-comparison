import csv
import json
import time
from kafka import KafkaProducer
import itertools


class ACCProducer:
    """
    This class is responsible for producing ACC data to a Kafka topic from a specified CSV file.
    """

    def __init__(self, bootstrap_servers, topic, csv_file, frequency, num_devices):
        """
        Initializes the producer with the specified parameters.

        :param bootstrap_servers: the bootstrap servers for the Kafka cluster
        :param topic: the topic to produce the data to
        :param csv_file: the path to the CSV file containing the data
        :param frequency: the frequency
        :param num_devices: the number of devices to simulate
        """
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        self.topic = topic
        self.csv_file = csv_file
        self.frequency = frequency
        self.num_devices = num_devices
        self.interval = 1.0 / frequency

    def produce(self, stop_event):
        """
        This method produces the data to the Kafka topic. It runs until the stop_event is set.

        :param stop_event: the stop event to stop the producer
        :return: None
        """
        with open(self.csv_file, 'r') as file:  # Open the CSV file
            csv_reader = csv.DictReader(file)  # Create a CSV reader
            csv_cycle = itertools.cycle(csv_reader)  # Create a cycle from the CSV reader

            while not stop_event.is_set():  # Run until the stop_event is set
                start_time = time.time()  # Get the current time (for calculating the latency)

                for device_id in range(self.num_devices):  # Iterate over the number of devices
                    row = next(csv_cycle)  # Get the next row from the CSV reader
                    message = {  # Create the message
                        'MAC_Addr': f"device_{device_id}_{row['MAC_Addr']}",
                        'Timestamp_CSV': row['Timestamp'],
                        'ACC_X': float(row['ACC_X']),
                        'ACC_Y': float(row['ACC_Y']),
                        'ACC_Z': float(row['ACC_Z']),
                        'Timestamp_produced': time.time()
                    }
                    self.producer.send(self.topic, message)  # Send the message to the Kafka topic

                elapsed_time = time.time() - start_time  # Calculate the latency
                sleep_time = self.interval - elapsed_time  # Calculate the sleep time
                if sleep_time > 0:  # If the sleep time is greater than 0, sleep for the remaining time
                    # (this is needed to mantain the desired sending frequency, e.g. 100 Hz)
                    time.sleep(sleep_time)

    def close(self):
        """
        Closes the producer.

        :return: None
        """
        self.producer.close()
