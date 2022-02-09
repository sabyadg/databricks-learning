# Databricks notebook source
# MAGIC 
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Reviewing and Visualizing data
# MAGIC #### Health tracker data
# MAGIC One common use case for working with Delta Lake is to collect and process Internet of Things (IoT) Data.
# MAGIC Here, we provide a mock IoT sensor dataset for demonstration purposes.
# MAGIC The data simulates heart rate data measured by a health tracker device.

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

# MAGIC %md-sandbox
# MAGIC <h2 style="color:red">Instructor Note</h2>
# MAGIC 
# MAGIC 
# MAGIC ## Transactions
# MAGIC 
# MAGIC In this notebook, we will focus on the **transactions** piece of the pipeline.
# MAGIC 
# MAGIC <img
# MAGIC      alt="Transactions"
# MAGIC      src=https://files.training.databricks.com/images/delta-lake-hands-on/01_big_pic_02-sst.jpeg
# MAGIC      width=600px
# MAGIC >
# MAGIC 
# MAGIC In a typical system, high flux event data will be delivered to the system
# MAGIC via a stream processing server like Apache Kafka. For educational purposes,
# MAGIC we have made this data available for download from static files.
# MAGIC The commands we ran in the ingest raw notebook are used to download the data into our system and are
# MAGIC intended to simulate the arrival of high flux event data.
# MAGIC Here, we will simulate the streaming of data that is normally done by a
# MAGIC stream processing platform like Apache Kafka by accessing files from the raw directory.
# MAGIC These files are multi-line JSON files and resemble the strings passed by Kafka.
# MAGIC A multi-line JSON file is one in which each line is a complete JSON object,
# MAGIC but the entire file itself is not a valid JSON file.
# MAGIC Each file consists of five users whose heart rate is measured each hour, 24 hours a day, every day.
# MAGIC Here is a sample of the data we will be using.
# MAGIC Each line is a string representing a valid JSON object and is similar to the kind of string
# MAGIC that would be passed by a Kafka stream processing server.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Health tracker data sample
# MAGIC 
# MAGIC ```
# MAGIC {"device_id":0,"heartrate":52.8139067501,"name":"Deborah Powell","time":1.5778368E9}
# MAGIC {"device_id":0,"heartrate":53.9078900098,"name":"Deborah Powell","time":1.5778404E9}
# MAGIC {"device_id":0,"heartrate":52.7129593616,"name":"Deborah Powell","time":1.577844E9}
# MAGIC {"device_id":0,"heartrate":52.2880422685,"name":"Deborah Powell","time":1.5778476E9}
# MAGIC {"device_id":0,"heartrate":52.5156095386,"name":"Deborah Powell","time":1.5778512E9}
# MAGIC {"device_id":0,"heartrate":53.6280743846,"name":"Deborah Powell","time":1.5778548E9}
# MAGIC ```
# MAGIC This shows a sample of the health tracker data we will be using. Note that each line is a valid JSON object.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Health tracker data schema
# MAGIC The data has the following schema:
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC | Column    | Type      |
# MAGIC |-----------|-----------|
# MAGIC | name      | string    |
# MAGIC | heartrate | double    |
# MAGIC | device_id | int       |
# MAGIC | time      | long      |

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Load the Data
# MAGIC Load the data as a Spark DataFrame from the raw directory.
# MAGIC This is done using the `.format("json")` option,
# MAGIC as well as a path to the `.load()` method.

# COMMAND ----------

# ANSWER
file_path = health_tracker + "raw/health_tracker_data_2020_1.json"

health_tracker_data_2020_1_df = spark.read.format("json").load(file_path)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Visualize Data
# MAGIC ### Step 1: Display the Data
# MAGIC Strictly speaking, this is not part of the ETL process, but displaying the data gives us a look at the data that we are working with.

# COMMAND ----------

display(health_tracker_data_2020_1_df)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Step 2: Configure the Visualization
# MAGIC Create a Databricks visualization to visualize the sensor data over time.
# MAGIC We have used the following plot options to configure the visualization:
# MAGIC ```
# MAGIC Keys: time
# MAGIC Series groupings: device_id
# MAGIC Values: heartrate
# MAGIC Aggregation: SUM
# MAGIC Display Type: Bar Chart
# MAGIC ```
# MAGIC Now that we have a better idea of the data we're working with, let's move on to create a Parquet-based table from this data.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <h2 style="color:red">Instructor Note</h2>
# MAGIC 
# MAGIC 
# MAGIC We note a few phenomena in the data:
# MAGIC - Sensor anomalies - Sensors cannot record negative heart rates, so any negative values in the data are anomalies.
# MAGIC - Wake/Sleep cycle - We notice that users have a consistent wake/sleep cycle alternating between steady high and low heart rates.
# MAGIC - Elevated activity - Some users have irregular periods of high activity.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2020 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>
