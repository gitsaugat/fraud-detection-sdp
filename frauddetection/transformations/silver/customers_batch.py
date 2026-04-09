
# from pyspark import pipelines as dp
# from pyspark.sql.functions import *
# from pyspark.sql.types import *
# from pyspark.sql.functions import current_timestamp, lit
# import pandas as pd 

# @dp.materialized_view(
#     name="frauddetection.silver.customers",
#     comment="Materialized customers data for fraud detection",
#     table_properties={
#         "quality": "silver",
#         "delta.enableChangeDataFeed": "true" # Enables the Auto CDC behavior you want
#     }
# )
# def customers_silver():

#     silver_table = "frauddetection.silver.customers"
#     bornze_table = "frauddetection.bronze.customers"


#     silver_df = spark.read.table(bornze_table)

#     silver_df = silver_df.withColumn("customer_id", col("CLIENTNUM"))\
#         .withColumn("customer_type",col("Attrition_Flag"))\
#         .withColumn("age",col("Customer_Age"))\
#         .withColumn("gender",col("Gender"))\
#         .withColumn("dependents",col("Dependent_count"))\
#         .withColumn("education",col("Education_Level"))\
#         .withColumn("marital_status",col("Marital_Status"))\
#         .withColumn("income",col("Income_Category"))\
#         .withColumn("card_type",col("Card_Category"))\
#         .withColumn("months_on_book",col("Months_on_book"))\
#         .withColumn("products",col("Total_Relationship_Count"))\
#         .withColumn("credit_limit",col("Credit_Limit"))\
#         .withColumn("debt_to_income",col("Avg_Open_To_Buy"))\
#         .withColumn("total_revolving_balance",col("Total_Revolving_Bal"))\
#         .withColumn("avg_utilization",col("Avg_Utilization_Ratio"))\
#         .withColumn("months_inactive",col("Months_Inactive_12_mon"))\
#         .withColumn("contacts",col("Contacts_Count_12_mon"))\
#         .withColumn("total_amt_change",col("Total_Amt_Chng_Q4_Q1"))\
#         .withColumn("total_trans_amt",col("Total_Trans_Amt"))\
#         .withColumn("total_trans_ct",col("Total_Trans_Ct"))\
#         .withColumn("total_ct_change",col("Total_Ct_Chng_Q4_Q1"))\
#         .withColumn("ingestTimeStamp",col("ingestTimeStamp"))\
#         .withColumn("ingestFileName",col("ingestFileName"))
                                        


#     silver_df=silver_df.select(
#         "customer_id",
#         "age",
#         "gender",
#         "marital_status",
#         "credit_limit",
#         "customer_type",
#         "card_type",
#         "income",
#         "education",
#         "dependents",
#         "debt_to_income",
#         "avg_utilization",
#         "months_inactive",
#         "months_on_book",
#         "products",
#         "total_amt_change",
#         "total_ct_change",
#         "total_revolving_balance",
#         "ingestTimeStamp",
#         "ingestFileName"
#         )


#     silver_df.count()

#     silver_df = silver_df.dropDuplicates()

   

#     return silver_df

