# Databricks notebook source
# MAGIC 
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Create a Parquet Table

# COMMAND ----------

# MAGIC %md
# MAGIC ## Notebook Configuration
# MAGIC 
# MAGIC Before you run this cell, make sure to add a unique user name to the file
# MAGIC <a href="$./includes/configuration" target="_blank">
# MAGIC includes/configuration</a>, e.g.
# MAGIC 
# MAGIC ```username = "yourfirstname_yourlastname"```

# COMMAND ----------

# MAGIC %run ./includes/configuration

# COMMAND ----------

# MAGIC %md
# MAGIC Reload data to DataFrame

# COMMAND ----------

file_path = health_tracker + "raw/health_tracker_data_2020_1.json"
health_tracker_data_2020_1_df = spark.read.format("json").load(file_path)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Create a Parquet Table

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <h2 style="color:red">Instructor Note</h2>
# MAGIC 
# MAGIC 
# MAGIC Now that we have used Databricks to preview the data, we'll work through the process of creating a Parquet-based data lake table. This table will be used in the next lesson to show the ease of converting existing Parquet-based tables to Delta tables.
# MAGIC The development pattern used to create a Parquet-based data lake table is similar to that used in creating a Delta table. There are a few issues that arise as part of the process, however. In particular, working with Parquet-based tables often requires table repairs to work with them.
# MAGIC In subsequent lessons, we'll see that creating a Delta table does not have the same issues.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Step 1: Make Idempotent
# MAGIC First, we remove the files in the `healthtracker/processed` directory.
# MAGIC 
# MAGIC Then, we drop the table we will create from the Metastore if it exists.
# MAGIC 
# MAGIC This step will make the notebook idempotent. In other words, it could be run more than once without throwing errors or introducing extra files.
# MAGIC 
# MAGIC 🚨 **NOTE** Throughout this lesson, we'll be writing files to the root location of the Databricks File System (DBFS). In general, best practice is to write files to your cloud object storage. We use DBFS root here for demonstration purposes.

# COMMAND ----------

dbutils.fs.rm(health_tracker + "processed", recurse=True)

spark.sql(
    f"""
DROP TABLE IF EXISTS health_tracker_processed
"""
)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Step 2: Transform the Data
# MAGIC We perform transformations by selecting columns in the following ways:
# MAGIC - use `from_unixtime` to transform `"time"`, cast as a `date`, and aliased to `dte`
# MAGIC - use `from_unixtime` to transform `"time"`, cast as a `timestamp`, and aliased to `time`
# MAGIC - `heartrate` is selected as is
# MAGIC - `name` is selected as is
# MAGIC - cast `"device_id"` as an integer aliased to `p_device_id`

# COMMAND ----------

# ANSWER
from pyspark.sql.functions import col, from_unixtime


def process_health_tracker_data(dataframe):
    return dataframe.select(
        from_unixtime("time").cast("date").alias("dte"),
        from_unixtime("time").cast("timestamp").alias("time"),
        "heartrate",
        "name",
        col("device_id").cast("integer").alias("p_device_id"),
    )


processedDF = process_health_tracker_data(health_tracker_data_2020_1_df)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Step 3: Write the Files to the processed directory
# MAGIC Note that we are partitioning the data by device id.
# MAGIC 
# MAGIC 1. use `.format("parquet")`
# MAGIC 1. partition by `"p_device_id"`

# COMMAND ----------

# ANSWER
(
    processedDF.write.mode("overwrite")
    .format("parquet")
    .partitionBy("p_device_id")
    .save(health_tracker + "processed")
)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Step 4: Register the Table in the Metastore
# MAGIC Next, use Spark SQL to register the table in the metastore.
# MAGIC Upon creation we specify the format as parquet and that the location where the parquet files were written should be used.

# COMMAND ----------

spark.sql(
    f"""
DROP TABLE IF EXISTS health_tracker_processed
"""
)

spark.sql(
    f"""
CREATE TABLE health_tracker_processed
USING PARQUET
LOCATION "{health_tracker}/processed"
"""
)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Step 5: Verify Parquet-based Data Lake table
# MAGIC 
# MAGIC Count the records in the `health_tracker_processed` Table

# COMMAND ----------

# ANSWER
health_tracker_processed = spark.read.table("health_tracker_processed")
health_tracker_processed.count()

# COMMAND ----------

# MAGIC %md
# MAGIC Note that the count does not return results.

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 6: Register the Partitions
# MAGIC 
# MAGIC Per best practice, we have created a partitioned table. However, if you create a partitioned table from existing data,
# MAGIC Spark SQL does not automatically discover the partitions and register them in the Metastore.
# MAGIC 
# MAGIC `MSCK REPAIR TABLE` will register the partitions in the Hive Metastore. Learn more about this command in <a href="https://docs.databricks.com/spark/latest/spark-sql/language-manual/sql-ref-syntax-ddl-repair-table.html" target="_blank">
# MAGIC the docs</a>.

# COMMAND ----------

spark.sql("MSCK REPAIR TABLE health_tracker_processed")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Step 7: Count the Records in the `health_tracker_processed` table
# MAGIC 
# MAGIC Count the records in the `health_tracker_processed` table.
# MAGIC 
# MAGIC With the table repaired and the partitions registered, we now have results.
# MAGIC We expect there to be 3720 records: five device measurements, 24 hours a day for 31 days.

# COMMAND ----------

# ANSWER
health_tracker_processed.count()


# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2020 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>
