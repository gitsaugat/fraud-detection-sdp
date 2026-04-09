from pyspark.sql.functions import *
from pyspark.sql.types import *
import pyspark.pipelines as sdp

@sdp.view
def customers_all():
    return spark.read.table("frauddetection.silver.customers") \


@sdp.materialized_view(name = "frauddetection.gold.dim_customers")
def dim_customers_view():
    return spark.read.table("customers_all") \
        .select(
            "customer_id",
            "age",
            "gender",
            "customer_type",
            "card_type",
            "education"
        ).dropDuplicates(subset = ["customer_id"])


@sdp.materialized_view(name = "frauddetection.gold.dim_card_types")
def dim_card_type():
    return spark.read.table("customers_all") \
        .select("card_type") \
        .dropDuplicates(["card_type"])



@sdp.materialized_view(name = "frauddetection.gold.dim_age_bands")
def dim_age_bands():
    return spark.read.table("customers_all") \
        .select(
            "customer_id",
            "age",
            when(col("age") < 25, lit("18-24"))
            .when(col("age") < 35, lit("25-34"))
            .when(col("age") < 50, lit("35-49"))
            .when(col("age") < 65, lit("50-64"))
            .otherwise(lit("65+")).alias("age_band")
        )


@sdp.materialized_view(name = "frauddetection.gold.dim_customer_type")
def dim_customer_type():
    return spark.read.table("customers_all") \
        .select("customer_type") \
        .dropDuplicates(subset = ["customer_type"])

