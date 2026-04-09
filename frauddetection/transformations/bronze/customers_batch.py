# from pyspark import pipelines as dp
# from pyspark.sql.functions import *
# from pyspark.sql.types import *
# from pyspark.sql.functions import current_timestamp, lit
# import pandas as pd 

# @dp.materialized_view(
#     name="customers",
#     comment="Materialized customer data for fraud detection",
#     table_properties={
#         "quality": "bronze",
#         "delta.enableChangeDataFeed": "true" # Enables the Auto CDC behavior you want
#     }
# )
# def customers():
#     # URL for the Azure Blob storage
#     customer_info = {
#         "table":"frauddetection.bronze.customers",
#         "file":"/customers/customers_5000.csv"
#     }



#     pd_df = pd.read_csv(
#         f"https://frauddetectionabcbank.blob.core.windows.net/bronze{customer_info["file"]}?sp=r&st=2026-03-25T03:25:25Z&se=2026-10-15T11:40:25Z&spr=https&sv=2024-11-04&sr=c&sig=xaoFm%2BaKYt%2BHY2ENUJZn3uIbDq8ctRLQusX9VBsRxPI%3D"
#     )


#     df = spark.createDataFrame(pd_df)\
#         .withColumn("ingestTimeStamp",current_timestamp())\
#             .withColumn("ingestFileName",lit(customer_info["file"]))

#     return df 




