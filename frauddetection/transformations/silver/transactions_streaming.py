from pyspark.sql.functions import *
from pyspark.sql.types import *
import pyspark.pipelines as dp 


schema = StructType([
    StructField("txn_id", StringType()),
    StructField("user_id", LongType()),
    StructField("amount", DoubleType()),
    StructField("timestamp", StringType()), 
    StructField("location", StringType())
])

# clean bronze streaming transactions and write to silver table
@dp.table(name="frauddetection.silver.streaming_transactions")
def silver_streaming_transactions():

    df = spark.readStream.table("frauddetection.bronze.streaming_transactions")
    
    df = df.select(
    from_json(col("transactions").cast("string"), schema).alias("data"),
    col("ingestTimeStamp")).select(
                "data.*",
                "ingestTimeStamp"
            )

    df = df.withColumn("timestamp", to_timestamp(col("timestamp")))

    df = df.withWatermark("timestamp", "2 minutes")\
        .dropDuplicates(subset = ("txn_id",))

    df = df.withColumn("customer_id",col("user_id")) \
    .withColumn("transaction_id",col("txn_id"))\
            .select("*").drop("user_id","txn_id")
    

    return df


dp.create_streaming_table("frauddetection.silver.all_transactions")

@dp.append_flow(
    target="frauddetection.silver.all_transactions"
)
def transactions_batch():

    df = spark.readStream.table("frauddetection.silver.transactions")

    return df.withColumn("source", lit("batch"))

@dp.append_flow(
    target="frauddetection.silver.all_transactions"
)
def transactions_stream():

    df = spark.readStream.table("frauddetection.silver.streaming_transactions")

    return df.withColumn("source", lit("event-hubs"))
