from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, FloatType, StringType
import logging
import time
import uuid

logger = logging.getLogger(__name__)


class SparkStreamingConsumer:
    def __init__(self, spark, bootstrap_servers='localhost:9092', topic='input-topic'):
        self.spark = spark
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.streaming_query = None
        self.is_running = False
        self.performance_metrics = None
        self.group_id = f"spark-kafka-consumer-{uuid.uuid4()}"

    def start(self, performance_metrics):
        self.performance_metrics = performance_metrics
        self.is_running = True

        schema = StructType([
            StructField("Temperature", FloatType(), True),
            StructField("Batt_level", StringType(), True)
        ])

        df = self.spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", self.bootstrap_servers) \
            .option("subscribe", self.topic) \
            .option("startingOffsets", "earliest") \
            .option("kafka.group.id", self.group_id) \
            .option("failOnDataLoss", "false") \
            .load()

        parsed_df = df.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")

        def process_batch(df, epoch_id):
            batch_count = df.count()
            for _ in range(batch_count):
                start_time = time.time()
                time.sleep(0.001)  # 1 ms di ritardo
                self.performance_metrics.record_latency(start_time)
            logger.info(f"Processed batch {epoch_id} with {batch_count} messages")

        self.streaming_query = parsed_df.writeStream \
            .foreachBatch(process_batch) \
            .option("checkpointLocation", f"/tmp/checkpoint-{self.group_id}") \
            .start()

        while self.is_running:
            if self.performance_metrics.producer_finished.is_set() and self.streaming_query.lastProgress is not None:
                if self.streaming_query.lastProgress["numInputRows"] == 0:
                    logger.info("Producer finished and no new data. Stopping consumer.")
                    self.stop()
                    break
            time.sleep(1)

    def stop(self):
        self.is_running = False
        if self.streaming_query:
            self.streaming_query.stop()
        logger.info("Spark Streaming consumer stopped")