
from pyspark import pipelines as dp
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.functions import current_timestamp, lit
import pandas as pd 


from pyspark import pipelines as dp
from pyspark.sql.functions import *

@dp.table(
    name="frauddetection.gold.user_transactions_behavior",
    comment="User transaction behavior features (lagged, incremental)",
    table_properties={"quality": "gold"}
)
def user_behavior():

    df = spark.readStream.table("frauddetection.silver.all_transactions") \
        .withWatermark("timestamp", "30 minutes") \
        .filter(
            col("timestamp") < current_timestamp() - expr("INTERVAL 30 MINUTES")
        )

    return df.groupBy("customer_id").agg(
        max("amount").alias("max_amount"),
        min("amount").alias("min_amount"),
        avg("amount").alias("avg_amount"),
        count("*").alias("transactions_count"),
        coalesce(stddev("amount"), lit(0)).alias("std_amount")
    )

@dp.table(
    name="frauddetection.gold.user_locations",
    comment="User location-based features (lagged, incremental)",
    table_properties={"quality": "gold"}
)
def user_locations():

    df = spark.readStream.table("frauddetection.silver.all_transactions") \
        .withWatermark("timestamp", "30 minutes") \
        .filter(
            col("timestamp") < current_timestamp() - expr("INTERVAL 30 MINUTES")
        )

    return df.groupBy("customer_id", "location").agg(
        count("*").alias("transactions_count"),
        avg("amount").alias("avg_amount")
    )

@dp.materialized_view(name="frauddetection.gold.dim_time")
def dim_time():

    df = spark.read.table("frauddetection.silver.all_transactions")

    return df.select(
        col("timestamp"),
        hour("timestamp").alias("hour"),
        dayofmonth("timestamp").alias("day"),
        month("timestamp").alias("month"),
        year("timestamp").alias("year"),
        date_format("timestamp", "EEEE").alias("day_of_week"),
        when(hour("timestamp").between(0,6), "night")
        .when(hour("timestamp").between(7,12), "morning")
        .when(hour("timestamp").between(13,18), "afternoon")
        .otherwise("evening").alias("time_bucket")
    ).dropDuplicates(["timestamp"])


@dp.materialized_view(name="frauddetection.gold.fact_customer_daily")
def fact_customer_daily():

    df = spark.read.table("frauddetection.silver.all_transactions")

    return df.groupBy(
        col("customer_id"),
        to_date("timestamp").alias("date")
    ).agg(
        count("*").alias("txn_count"),
        sum("amount").alias("total_spend"),
        avg("amount").alias("avg_spend"),
        max("amount").alias("max_spend")
    )

@dp.materialized_view(name="frauddetection.gold.dim_amount_band")
def dim_amount_band():

    df = spark.read.table("frauddetection.silver.all_transactions")

    return df.select(
        "transaction_id",
        "amount",
        when(col("amount") < 100, "low")
        .when(col("amount") < 500, "medium")
        .when(col("amount") < 1000, "high")
        .otherwise("very_high").alias("amount_band")
    )