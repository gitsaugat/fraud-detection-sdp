
# from pyspark import pipelines as dp
# from pyspark.sql.functions import *
# from pyspark.sql.types import *
# from pyspark.sql.functions import current_timestamp, lit
# import pandas as pd 

# @dp.materialized_view(
#     name="frauddetection.silver.transactions",
#     comment="Materialized silver transactions data for fraud detection",
#     table_properties={
#         "quality": "silver",
#         "delta.enableChangeDataFeed": "true" # Enables the Auto CDC behavior you want
#     }
# )
# def transactions_silver():

#     silver_table = "frauddetection.silver.transactions"
#     bornze_table = "frauddetection.bronze.transactions"

#     silver_df = spark.read.table(bornze_table)

#     silver_df = silver_df.dropDuplicates(
#         subset = ["txn_id"]
#     )

#     silver_df = silver_df.withColumn(
#         "transaction_id",col("txn_id")
#     ).withColumn(
#         "customer_id",col("user_id")
#     )

#     silver_df = silver_df.select(
#         col("transaction_id"),
#         col("customer_id"),
#         col("amount"),
#         col("timestamp"),
#         col("location"),
#         col("ingestTimeStamp"),
#         col("ingestFileName")
#     )

#     silver_df = silver_df.withColumn(
#         "amount", round(col("amount"), 2)
#     )

#     silver_df = silver_df.withColumn(
#         "timestamp", col("timestamp").cast("timestamp")
#     )
#     return silver_df


  
