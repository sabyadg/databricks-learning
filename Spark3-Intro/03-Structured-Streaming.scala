// Databricks notebook source
// MAGIC %md # Structured Streaming
// MAGIC 
// MAGIC Apache Spark 2.x introduced the Structured Streaming application model:
// MAGIC * We treat a streaming source just like a (table) data frame source
// MAGIC * Assume that this table is "append-only" so we focus on working with new rows, the source won't delete or change rows we've already read
// MAGIC   * Although the stream might include new "update" rows associated with a new timestamp (e.g., time series, change data capture, etc.)
// MAGIC * Spark uses its SQL / Dataframe APIs and internal optimizers to handle our queries ... which now represent a stream of results
// MAGIC * Since our query output is also a stream, we need to send it somewhere: a sink
// MAGIC 
// MAGIC __If we use the right sources and sinks, and play by the rules, we get a very easy-to-code streaming system with fault tolerant, exactly-once end-to-end processing__
// MAGIC 
// MAGIC We'll look at those source/sinks/rules later, but first we'll start with a really simple (but less robust) example.
// MAGIC 
// MAGIC Let's read in the live, real-time stream of global edits to Wikipedia.
// MAGIC 
// MAGIC We can read them in JSON structures from a server/port:

// COMMAND ----------

// MAGIC %sh timeout 1 nc 54.213.33.240 9002

// COMMAND ----------

// MAGIC %sql SET spark.sql.shuffle.partitions = 3
// MAGIC 
// MAGIC -- fewer tasks for small volume stream

// COMMAND ----------

import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming._
import spark.implicits._

// COMMAND ----------

val lines = spark.readStream
  .format("socket")
  .option("host", "54.213.33.240")
  .option("port", 9002)
  .load()

val edits = lines.select(json_tuple('value, "channel", "timestamp", "isRobot", "isAnonymous"))

// COMMAND ----------

display(edits)

// COMMAND ----------

// MAGIC %md We'll try something a little more complex.
// MAGIC 
// MAGIC * Rename the columns something more useful than c0, c1, etc.
// MAGIC * Interpret the time as SQL timestamp (it was a raw string earlier)
// MAGIC * Create 10-second windows over the timestamp
// MAGIC * Transform the stream by grouping by channel and time, then counting edits

// COMMAND ----------

val lines = spark.readStream.format("socket").option("host", "54.213.33.240").option("port", 9002).load()

lines.select(json_tuple('value, "channel", "timestamp", "page"))
     .selectExpr("c0 as channel", "cast(c1 as timestamp) as time", "c2 as page")
     .createOrReplaceTempView("edits")

val editCounts = spark.sql("""SELECT count(*), channel, date_format(window(time, '10 seconds').start, 'HH:mm:ss') as time 
                              FROM edits 
                              GROUP BY channel, window(time, '10 seconds')
                              ORDER BY time""")

// COMMAND ----------

display(editCounts)

// COMMAND ----------

// MAGIC %md There's a lot of magic going on in that example!
// MAGIC 
// MAGIC Consider what happens when a record comes in late ... meaning it has a timestamp from a few seconds earlier.
// MAGIC 
// MAGIC The `GROUP BY` means that this data needs to be aggregated with the *previously received records having a corresponding timestamp*
// MAGIC 
// MAGIC In other words, the data need to be processed based on their indicated order not their received order, and that means *changing an aggregation that was already reported to the output sink*
// MAGIC 
// MAGIC The important takeaways in this overview are that
// MAGIC 
// MAGIC * Spark is designed to do exactly this, and to do it in an optimized and fault-tolerant fashion
// MAGIC * The performance or suitability of this approach depends on where the data needs to go (the sink)

// COMMAND ----------

// MAGIC %md #####What do we need to do to leverage the most powerful included features in a production environment?
// MAGIC E.g.,
// MAGIC * Fault tolerance
// MAGIC * Available source/sink strategies
// MAGIC * Incremental query optimization
// MAGIC 
// MAGIC https://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html
// MAGIC 
// MAGIC Adding a library to Spark on Databricks is easy -- start with Create -> Library in the file GUI. We can look for `spark-sql-kafka-*` ... although in our current Databricks environment, Structured Streaming Kafka access is preloaded.

// COMMAND ----------

// Reading from Kafka returns a DataFrame with the following fields:
//
// key           - data key (i.e., for key-value records; we aren't using it here)
// value         - data, in base64 encoded binary format. This is our JSON payload. We'll need to cast it to STRING.
// topic         - Kafka topic. In this case, the topic is the same as the "wikipedia" field, so we don't need it.
// partition     - Kafka topic partition. This server only has one partition, so we don't need this information.
// offset        - Kafka topic-partition offset value -- this is essentially the message's unique ID. Take a look at the values Kafka produces here!
// timestamp     - not used
// timestampType - not used

val kafkaDF = spark.readStream
  .format("kafka")
  .option("kafka.bootstrap.servers", "54.213.33.240:9092")
  .option("subscribe", "en,pl,de")
  .load()

val editsDF = kafkaDF.select($"value".cast("string").as("value"))

// COMMAND ----------

display(editsDF)

// COMMAND ----------

import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

/*
val schema = StructType(List(
                StructField("wikipedia", StringType, false),
                StructField("timestamp", TimestampType, false),
                StructField("page", StringType, false),
                StructField("isRobot", BooleanType, false)
             ))

 -- NOPE! You can do this, but you don't have to!
*/

val schema = StructType.fromDDL("wikipedia STRING, timestamp TIMESTAMP, page STRING, isRobot BOOLEAN")

val extractedDF = editsDF.select(from_json('value, schema) as "json")

// COMMAND ----------

display(extractedDF)

// COMMAND ----------

extractedDF.printSchema

// COMMAND ----------

val windowColDF = extractedDF.select($"json.wikipedia", window($"json.timestamp", "10 seconds") as "time_window")

// COMMAND ----------

val prettyView = windowColDF.groupBy('time_window, 'wikipedia).count()

display(prettyView)

// COMMAND ----------

dbutils.fs.rm("/tmp/demo", true)
dbutils.fs.rm("/tmp/ck", true)

val query = windowColDF.writeStream
  .option("checkpointLocation", "/tmp/ck")
  .format("parquet")
  .start("/tmp/demo")

// COMMAND ----------

val prettyView = spark.read.parquet("/tmp/demo").groupBy('time_window, 'wikipedia).count()

display(prettyView)

// COMMAND ----------

// MAGIC %md This is the modern way to use Spark streaming, making continuous applications much easier to engineer and more flexible toward changing business requirements.

// COMMAND ----------

query.stop

// COMMAND ----------

// MAGIC %md ### Spark 2.3+: Continuous Streaming with Millisecond Latency
// MAGIC 
// MAGIC <img src="https://i.imgur.com/5ABtu2C.png" width=800>
// MAGIC 
// MAGIC Read more about continuous-mode streaming:
// MAGIC * https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#continuous-processing
// MAGIC * https://databricks.com/blog/2018/03/20/low-latency-continuous-processing-mode-in-structured-streaming-in-apache-spark-2-3-0.html
