from pyspark.sql.functions import *
from pyspark.sql.types import *
import pyspark.pipelines as sdp


@sdp.table(name = "frauddetection.gold.fact_fraud_detection")
def fact_fraud_detection():

    df = spark.readStream.table("frauddetection.silver.streaming_transactions")\
      .withColumn("source",lit("event-hubs")) # get the streaming transactions
    
    # get the past behaviour
    user_behavior_table = spark.read.table("frauddetection.gold.user_transactions_behavior")
    
    
    # get user locations
    user_locations_table = spark.read.table("frauddetection.gold.user_locations")\
        .withColumnRenamed("transactions_count", "location_transaction_count")\
          .withColumnRenamed("avg_amount","location_user_avg_amount")

    # join behaviour table to main df

    df = df.join(
        user_behavior_table, 
        on = "customer_id", 
        how = "left"
    )
    # join location table to main df
    df = df.join(
        user_locations_table, 
        on = ["customer_id","location"], 
        how = "left"
    )
    #check fraud 
    df = df.withColumn( "is_location_anomaly", col("location_transaction_count").isNull())\
      .withColumn("is_suspicious_location_amount", col("is_location_anomaly") & (col("amount") > col("avg_amount") * 2) ). \
        withColumn("is_amount_extreme",col("amount") > col("max_amount") * 2 )\
          .withColumn( "is_stat_anomaly", col("amount") > (col("avg_amount") + 2 * col("std_amount")))\
            .withColumn("source", lit("event-hubs"))

    df = df.withColumn(
        "fraud_score",
        col("is_location_anomaly").cast("int") * 1 +
        col("is_suspicious_location_amount").cast("int") * 2 +
        col("is_amount_extreme").cast("int") * 2 +
        col("is_stat_anomaly").cast("int") * 2
    )

    df = df.withColumn(
    "fraud_flag",
        when(col("fraud_score") == 0, 1)
        .when(col("fraud_score") <= 2, 2)
        .when(col("fraud_score") <= 4, 3)
        .when(col("fraud_score") <= 6, 4)
        .when(col("fraud_score") <= 8, 5)
        .otherwise(6)
    )


    df = df.select(
      "customer_id",
      "transaction_id",
      "location",
      "amount",
      "timestamp",
      "source",
      "is_location_anomaly",
      "is_suspicious_location_amount",
      "is_amount_extreme",
      "is_stat_anomaly",
      "fraud_score",
      "fraud_flag"
    )
    return df 


  
  
