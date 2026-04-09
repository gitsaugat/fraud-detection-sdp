# Databricks notebook source
# MAGIC %sql
# MAGIC drop table if exists frauddetection.bronze.customers;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from frauddetection.silver.all_transactions_unified where source = "event-hubs" limit 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from frauddetection.silver.streaming_transactions;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from frauddetection.silver.transactions;

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists frauddetection.silver.all_transactions_unified

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from frauddetection.silver.customers limit 100;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from frauddetection.silver.all_transactions where source = "event-hubs" limit 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from frauddetection.gold.fact_fraud_detection limit 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from frauddetection.gold.user_transactions_behaviour limit 10;
