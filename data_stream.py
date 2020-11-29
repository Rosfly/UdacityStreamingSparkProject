"""

Spark Streaming Example
=======================

This example uses Kafka to deliver a stream of words to a Python word count program

docker-compose up -d
docker ps
docker exec -it spark_project_kafka0_1 bash

Create a kafka topic (not needed here)
kafka-topics --create --bootstrap-server PLAINTEXT://kafka0:9092 --topic "wc1" --partitions 1 --replication-factor 1

Check it in the list of available topics
kafka-topics --list --bootstrap-server PLAINTEXT://kafka0:9092

Start a Kafka producer where you type and publish (not needed here)
kafka-console-producer --broker-list localhost:9092 --topic wc1

Check your results in another console (go into same container)
kafka-console-consumer --topic "<TOPICNAME>" --bootstrap-server PLAINTEXT://kafka0:9092


Now in VSCode activate env like venv_spark2.4
export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64/jre

-- Use spark-submit if progress report needed, 
spark-submit --jars jars/spark-sql-kafka-0-10_2.11-2.4.0.jar,jars/kafka-clients-1.1.0.jar data_stream.py
-- otherwise use standard python file.py 
to get rid of all spark stuff on output!


see databricks help on timestamp kafka / 1000 needed
https://docs.databricks.com/spark/latest/structured-streaming/examples.html
"""
import logging
import json
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, StructField, TimestampType, IntegerType
#import pyspark.sql.functions as psf
from pyspark.sql import functions as psf

TOPIC_NAME = "sf.police.calls-for-service.v8"
BOOTSTRAP = "PLAINTEXT://localhost:9092"

# subfolder jars contains manually downloaded jars from mvn for local execution
LOCALPATH = "jars/"
LIBRARY_1 = LOCALPATH + "spark-sql-kafka-0-10_2.11-2.4.0.jar"
LIBRARY_2 = LOCALPATH + "kafka-clients-1.1.0.jar"

# TODO Create a schema for incoming resources
schema = StructType([
        StructField("crime_id", StringType(), False),
        StructField("original_crime_type_name", StringType(), True),
        StructField("report_date", TimestampType(), True),
        StructField("call_date", TimestampType(), True),
        StructField("offense_date", TimestampType(), True),
        StructField("call_time", StringType(), True),
        StructField("call_date_time", TimestampType(), True),
        StructField("disposition", StringType(), True),
        StructField("address", StringType(), True),
        StructField("city", StringType(), True),
        StructField("state", StringType(), True),
        StructField("agency_id", StringType(), True),
        StructField("address_type", StringType(), True),
        StructField("common_location", StringType(), True)
    ])

radio_code_schema = StructType([
        StructField("disposition_code",StringType(),True),
        StructField("description",StringType(),True)
    ])

def run_spark_job(spark):

    # TODO Create Spark Configuration
    # Create Spark configurations with max offset of 200 per trigger
    # set up correct bootstrap server and port
    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", BOOTSTRAP) \
        .option("subscribe", TOPIC_NAME) \
        .option("startingOffsets", "earliest") \
        .option("maxOffsetsPerTrigger", 200) \
        .option("maxRatePerPartition", 10)\
        .option("stopGracefullyOnShutdown", "true") \
        .load()
    # Show schema for the incoming resources for checks
    df.printSchema()

    #.option("maxRatePerPartition", 1) \ max number of messages per partition per batch

    # TODO extract the correct column from the kafka input resources
    # Take only value and convert it to String
    kafka_df = df.selectExpr("CAST(value AS STRING)")

    # # DEBUG: check the correct json read=============
    # query = kafka_df\
    # .writeStream\
    # .outputMode('update')\
    # .format('console')\
    # .start()

    # query.awaitTermination()
    # END of DEBUG ====================================

    service_table = kafka_df\
        .select(psf.from_json(psf.col('value'), schema).alias("DF"))\
        .select("DF.*")

    # # DEBUG: check the correct json read=============
    # query = service_table\
    # .writeStream\
    # .outputMode('update')\
    # .format('console')\
    # .start()
    # query.awaitTermination()
    # # END of DEBUG ====================================

    # # TODO select original_crime_type_name and disposition
    distinct_table = service_table.select("original_crime_type_name","disposition")
    # query = distinct_table\
    # .writeStream\
    # .outputMode('update')\
    # .format('console')\
    # .start()
    # query.awaitTermination()


    # # count the number of original crime type
    agg_df = distinct_table\
        .groupBy("original_crime_type_name")\
        .count()\
        .sort('count',ascending=False)

    # # TODO Q1. Submit a screen shot of a batch ingestion of the aggregation
    # # TODO write output stream: for aggregation outputMode COMPLETE
    query = agg_df\
        .writeStream\
        .trigger(processingTime="30 seconds")\
        .outputMode('complete')\
        .format('console')\
        .start()

    # TODO attach a ProgressReporter
    query.awaitTermination()


    # # TODO get the right radio code json path
    radio_code_json_filepath = "./radio_code_data" # radio_code.json inside
    radio_code_df = spark\
        .readStream\
        .option("multiline","true")\
        .schema(radio_code_schema)\
        .json(radio_code_json_filepath)

    # # # DEBUG: check the correct json read=============
    # query = radio_code_df\
    #     .writeStream\
    #     .outputMode('append')\
    #     .format('console')\
    #     .start()
    # query.awaitTermination()
    # # # END of DEBUG: check the correct json read=============

    # # clean up your data so that the column names match on radio_code_df and agg_df
    # # we will want to join on the disposition code

    # # TODO rename disposition_code column to disposition
    radio_code_df = radio_code_df.withColumnRenamed("disposition_code", "disposition")

    # query = radio_code_df\
    #     .writeStream\
    #     .outputMode('append')\
    #     .format('console')\
    #     .start()
    # query.awaitTermination()

    # # TODO join on disposition column
    join_query = distinct_table.join(radio_code_df,"disposition")

    # query2 = join_query\
    #     .writeStream\
    #     .trigger(processingTime="60 seconds")\
    #     .outputMode('append')\
    #     .format('console')\
    #     .start()
    # query2.awaitTermination()


if __name__ == "__main__":
    logger = logging.getLogger(__name__)

    # TODO Create Spark in Standalone mode
    # configs needed to reference the manually downloaded jars
    spark = SparkSession \
        .builder \
        .master("local[*]") \
        .config("spark.jars",f"{LIBRARY_1},{LIBRARY_2}")\
        .config("spark.executor.extraClassPath", f"{LIBRARY_1},{LIBRARY_2}")\
        .config("spark.executor.extraLibrary", f"{LIBRARY_1},{LIBRARY_2}")\
        .config("spark.driver.extraClassPath", f"{LIBRARY_1},{LIBRARY_2}")\
        .appName("KafkaSparkStructuredStreamingV8") \
        .getOrCreate()

    # .master("local[*]") uses ALL available cores" see UI the tab "Executors" >> CORES, [1] 1 core
    logger.info("Spark started")

    run_spark_job(spark)

    spark.stop()
