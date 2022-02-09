# Databricks notebook source
# MAGIC 
# MAGIC %md
# MAGIC **Define Data Paths**

# COMMAND ----------

# ANSWER
username = "dbacademy"

# COMMAND ----------

health_tracker = f"/dbacademy/{username}/lakehouse-with-delta-lake-dd/health-tracker/"

# COMMAND ----------

# MAGIC %md
# MAGIC **Configure Database**

# COMMAND ----------

spark.sql(f"CREATE DATABASE IF NOT EXISTS dbacademy_{username}")
spark.sql(f"USE dbacademy_{username}")

# COMMAND ----------

# MAGIC %md
# MAGIC **Import Utility Functions**

# COMMAND ----------

# MAGIC %run ./utilities
