from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, FloatType, TimestampType
import time
import logging

class SparkStreamingConsumer:
    def __init__(self, bootstrap_servers, topics):
        self.bootstrap_servers = bootstrap_servers
        self.topics = topics
        self.spark = SparkSession.builder \
            .appName("FitbandDataConsumer") \
            .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2") \
            .config("spark.metrics.conf.*.sink.prometheusServlet.class",
                    "org.apache.spark.metrics.sink.PrometheusServlet") \
            .config("spark.metrics.conf.*.sink.prometheusServlet.path", "/metrics") \
            .config("spark.ui.prometheus.enabled", "true") \
            .getOrCreate()
        self.spark.sparkContext.setLogLevel("ERROR")
        self.logger = logging.getLogger('SparkStreamingConsumer')
        self.logger.setLevel(logging.DEBUG)

    def create_stream(self, topic, schema=StructType([])):
        self.logger.info(f'Creating stream for topic: {topic} with schema: {schema}')
        stream_df = self.spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", self.bootstrap_servers) \
            .option("subscribe", topic) \
            .option("startingOffsets", "earliest") \
            .load() \
            .select(from_json(col("value").cast("string"), schema).alias("data")) \
            .select("data.*")
        if stream_df.isStreaming:
            self.logger.info(f'Stream created successfully for topic: {topic}')
        else:
            self.logger.error(f'Failed to create stream for topic: {topic}')
        return stream_df

    def process_stream(self, df, topic):
        self.logger.info(f'Processing stream for topic: {topic}')
        query = df \
            .writeStream \
            .outputMode("append") \
            .format("console") \
            .start()
        return query

    def run(self):
        ppg_schema = StructType([
            StructField("MAC_Addr", StringType()),
            StructField("Timestamp", TimestampType()),
            StructField("PPG1", FloatType()),
            StructField("PPG2", FloatType()),
            StructField("PPG3", FloatType()),
            StructField("PPG4", FloatType()),
            StructField("PPG5", FloatType()),
            StructField("PPG6", FloatType())
        ])

        acc_schema = StructType([
            StructField("MAC_Addr", StringType()),
            StructField("Timestamp", TimestampType()),
            StructField("ACC_X", FloatType()),
            StructField("ACC_Y", FloatType()),
            StructField("ACC_Z", FloatType())
        ])

        temp_batt_schema = StructType([
            StructField("MAC_Addr", StringType()),
            StructField("Timestamp", TimestampType()),
            StructField("Temperature", FloatType()),
            StructField("Batt_level", FloatType()),
            StructField("Batt_status", StringType())
        ])

        ppg_stream = self.create_stream("ppg-topic", ppg_schema)
        acc_stream = self.create_stream("acc-topic", acc_schema)
        temp_batt_stream = self.create_stream("temp-batt-topic", temp_batt_schema)

        ppg_query = self.process_stream(ppg_stream, "ppg-topic")
        acc_query = self.process_stream(acc_stream, "acc-topic")
        temp_batt_query = self.process_stream(temp_batt_stream, "temp-batt-topic")

        self.logger.info('Started all streams successfully')
        self.logger.info(ppg_query)
        self.logger.info(acc_query)
        self.logger.info(temp_batt_query)

        try:
            while True:
                self.logger.info('Streaming is running')
                time.sleep(10)
        except KeyboardInterrupt:
            self.logger.info('Stopping all streams')
            ppg_query.stop()
            acc_query.stop()
            temp_batt_query.stop()


if __name__ == "__main__":
    consumer = SparkStreamingConsumer("localhost:29092", ["ppg-topic", "acc-topic", "temp-batt-topic"])
    consumer.run()