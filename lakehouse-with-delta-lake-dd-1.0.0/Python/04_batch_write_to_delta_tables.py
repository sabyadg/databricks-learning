# Databricks notebook source
# MAGIC 
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Batch Write to Delta Tables
# MAGIC 
# MAGIC **Objective:** Append files to an existing Delta Table

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <h2 style="color:red">Instructor Note</h2>
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC Appending Files to an Existing Delta Table
# MAGIC 
# MAGIC There are two patterns for modifying existing Delta tables:
# MAGIC * appending files to an existing directory of Delta files
# MAGIC * merging a set of updates and insertions
# MAGIC 
# MAGIC In this lesson, we explore the first.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Notebook Configuration
# MAGIC 
# MAGIC Before you run this cell, make sure to add a unique user name to the file
# MAGIC <a href="$./includes/configuration" target="_blank">
# MAGIC includes/configuration</a>, e.g.
# MAGIC 
# MAGIC ```
# MAGIC username = "yourfirstname_yourlastname"
# MAGIC ```

# COMMAND ----------

# MAGIC %run ./includes/configuration

# COMMAND ----------

# MAGIC %run ./includes/main/python/operations

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Step 1: Load New Data
# MAGIC 
# MAGIC Within the context of our data ingestion pipeline, this is the addition of new raw files to our Single Source of Truth.
# MAGIC 
# MAGIC We begin by loading the data from the file `health_tracker_data_2020_2.json`, using the `.format("json")` option as before.

# COMMAND ----------

file_path = health_tracker + "raw/health_tracker_data_2020_2.json"

health_tracker_data_2020_2_df = (
  spark.read
  .format("json")
  .load(file_path)
)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Step 2: Transform the Data
# MAGIC We perform the same data engineering on the data:
# MAGIC - Use the from_unixtime Spark SQL function to transform the unixtime into a time string
# MAGIC - Cast the time column to type timestamp to replace the column time
# MAGIC - Cast the time column to type date to create the column dte

# COMMAND ----------

processedDF = process_health_tracker_data(spark, health_tracker_data_2020_2_df)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Step 3: Append the Data to the `health_tracker_processed` Delta table
# MAGIC We do this using `.mode("append")`. Note that it is not necessary to perform any action on the Metastore.

# COMMAND ----------

(processedDF.write
 .mode("append")
 .format("delta")
 .save(health_tracker + "processed"))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### View the Commit Using Time Travel
# MAGIC Delta Lake can query an earlier version of a Delta table using a feature known as time travel. Here, we query the data as of version 0, that is, the initial conversion of the table from Parquet.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Step 1: View the table as of Version 0
# MAGIC This is done by specifying the option `"versionAsOf"` as 0. When we time travel to Version 0, we see **only** the first month of data, five device measurements, 24 hours a day for 31 days.

# COMMAND ----------

(spark.read
 .option("versionAsOf", 0)
 .format("delta")
 .load(health_tracker + "processed")
 .count())

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Step 2: Count the Most Recent Version
# MAGIC When we query the table without specifying a version, it shows the latest version of the table and includes the new records added.
# MAGIC When we look at the current version, we expect to see two months of data: January 2020 and February 2020. 
# MAGIC 
# MAGIC The data should include the following records: 
# MAGIC 
# MAGIC ``` 5 devices * 60 days * 24 hours = 7200 records```
# MAGIC 
# MAGIC Note that the range of data includes the month of February during a leap year. 29 days in Feb plus 31 in January gives us 60 days total.

# COMMAND ----------

(spark.read
 .format("delta")
 .load(health_tracker + "processed")
 .count())

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Note that we do not have a correct count. We are missing 72 records.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2020 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>
