# from pyspark import pipelines as dp
# from pyspark.sql.functions import *
# from pyspark.sql.types import *
# from pyspark.sql.functions import current_timestamp, lit
# import pandas as pd 

# @dp.materialized_view(
#     name="transactions",
#     comment="Materialized transactions data for fraud detection",
#     table_properties={
#         "quality": "bronze",
#         "delta.enableChangeDataFeed": "true" # Enables the Auto CDC behavior you want
#     }
# )
# def transactions():

   
#     transactions_info =    {
#             "table":"frauddetection.bronze.transactions",
#             "file":"/transactions/transactions_500k.csv"
#         }

    
#     pd_df = pd.read_csv(
       
#     )


#     df = spark.createDataFrame(pd_df)\
#         .withColumn("ingestTimeStamp",current_timestamp())\
#             .withColumn("ingestFileName",lit(transactions_info["file"]))

#     return df



