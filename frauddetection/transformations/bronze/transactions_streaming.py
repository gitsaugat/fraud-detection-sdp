from pyspark import pipelines as dp
from pyspark.sql.functions import *
from pyspark.sql.types import *

EH_CONN_STR = spark.conf.get("connection_string_eventhubs")
EH_NAMESPACE   = spark.conf.get("eh_namespace")
EH_NAME        = spark.conf.get("eh_name")


KAFKA_OPTIONS = {
  "kafka.bootstrap.servers"  : f"saugatubereventsde.servicebus.windows.net:9093",
  "subscribe": "uberdetopic",
  "kafka.sasl.mechanism"     : "PLAIN",
  "kafka.security.protocol"  : "SASL_SSL",
  "kafka.sasl.jaas.config"   : f"kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username=\"$ConnectionString\" password=\"{EH_CONN_STR}\";",
  "kafka.request.timeout.ms" : 10000,
  "kafka.session.timeout.ms" : 10000,
  "maxOffsetsPerTrigger"     : 10000,
  "failOnDataLoss"           : 'true',
  "startingOffsets"          : 'earliest',
  "kafka.group.id": "$Default"
}

@dp.table(
  name="frauddetection.bronze.streaming_transactions",
  comment = "reading raw stream data from events hub"
  )
def transactions_streaming():
    df = spark.readStream.format("kafka")\
                .options(**KAFKA_OPTIONS)\
                .option("checkpointLocation", "/Volumes/frauddetection/bronze/transactions_streaming_checkpoint")\
                .load()

    df = df.withColumn("transactions",col("value").cast("string"))
    df = df.withColumn("ingestTimeStamp",current_timestamp()) 
    
    return df






