import csv
import logging

logger = logging.getLogger(__name__)

class CSVReader:
    def __init__(self, file_path):
        self.file_path = file_path
        self.file = None
        self.reader = None

    def open(self):
        try:
            self.file = open(self.file_path, 'r')
            self.reader = csv.DictReader(self.file)
            logger.info(f"Opened CSV file: {self.file_path}")
        except IOError as e:
            logger.error(f"Error opening CSV file: {e}")
            raise

    def close(self):
        if self.file:
            self.file.close()
            logger.info(f"Closed CSV file: {self.file_path}")

    def __iter__(self):
        return self

    def __next__(self):
        if not self.reader:
            raise StopIteration

        try:
            row = next(self.reader)
            return {
                'Temperature': float(row['Temperature']),
                'Batt_level': row['Batt_level']
            }
        except StopIteration:
            self.close()
            raise
        except (ValueError, KeyError) as e:
            logger.error(f"Error parsing CSV row: {e}")
            return self.__next__()  # Skip this row and try the next one

    def reset(self):
        self.close()
        self.open()
        logger.info("Reset CSV reader to the beginning of the file")