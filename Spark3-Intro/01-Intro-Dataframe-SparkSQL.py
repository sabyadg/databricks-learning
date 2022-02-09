# Databricks notebook source
# MAGIC %md # First Steps with Apache Spark 3.x

# COMMAND ----------

# MAGIC %md #### Class Logistics and Operations
# MAGIC * ~5 hours
# MAGIC * Questions
# MAGIC * Breaks
# MAGIC * Databricks compared to Apache Spark
# MAGIC   * Working with the Databricks/Notebook environment
# MAGIC   * Alternative notebooks
# MAGIC     * Zeppelin https://zeppelin.apache.org/
# MAGIC     * Jupyter https://www.sicara.ai/blog/2017-05-02-get-started-pyspark-jupyter-notebook-3-minutes    
# MAGIC   * *Beware of anything involving Spark on your local device -- more on this later*

# COMMAND ----------

# MAGIC %md #### Topics
# MAGIC 
# MAGIC   * Background / Architecture
# MAGIC   * Querying Data with DataFrame, Dataset, SQL
# MAGIC   * A Bit on Internals, Job Execution, and Integrating Custom Logic
# MAGIC   * Brief Intro to ...
# MAGIC     * SparkML Machine Learning
# MAGIC     * Structured Streaming

# COMMAND ----------

dbutils.fs.ls("/databricks-datasets/amazon")

# COMMAND ----------

# MAGIC %md Where is this data? Currently, Amazon S3
# MAGIC 
# MAGIC In this version of Databricks, "DBFS" (a wrapper over S3 similar to EMRFS) is the default filesystem.
# MAGIC 
# MAGIC Other common defaults include HDFS, "local", another cloud-based object store like Azure Blob Storage, or a Kubernetes-friendly storage layer like Minio.

# COMMAND ----------

# MAGIC %sql SELECT * FROM parquet.`/databricks-datasets/amazon/data20K` 

# COMMAND ----------

# MAGIC %sql SELECT * FROM csv.`/databricks-datasets/atlas_higgs` 

# COMMAND ----------

# MAGIC %scala
# MAGIC val df = spark.sql("SELECT * FROM parquet.`/databricks-datasets/amazon/data20K`")

# COMMAND ----------

# MAGIC %python
# MAGIC sdf = spark.read.parquet("/databricks-datasets/amazon/data20K")

# COMMAND ----------

display(sdf)

# COMMAND ----------

sdf.createOrReplaceTempView("data2k")

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT * FROM data2k

# COMMAND ----------

# MAGIC %scala
# MAGIC df.show()

# COMMAND ----------



# COMMAND ----------

# MAGIC %scala
# MAGIC display(df)

# COMMAND ----------

# MAGIC %sql SELECT count(1), rating FROM parquet.`/databricks-datasets/amazon/data20K` GROUP BY rating

# COMMAND ----------

# MAGIC %sql SELECT * FROM parquet.`/databricks-datasets/amazon/data20K` WHERE rating = 1 AND review LIKE '%awesome%'

# COMMAND ----------

# MAGIC %md ### Introduction
# MAGIC * What is Spark?
# MAGIC   * Spark is a *distributed*, *data-parallel* compute engine ... with lots of other goodies around to make the work easier for common tasks
# MAGIC * How is Spark different from ... 
# MAGIC   * Hadoop?
# MAGIC   * RDBMSs?
# MAGIC * Brief History of Spark
# MAGIC   * Origin
# MAGIC   * Spark 1.x
# MAGIC   * SparkSQL, SparkML (~1.4+, 2.x)
# MAGIC   * 2017-2020 improvements and challenges
# MAGIC   * Spark 3.0

# COMMAND ----------

# MAGIC %md ### Key Spark 3.x Changes
# MAGIC *an opinionated list*
# MAGIC 
# MAGIC Major
# MAGIC 1. Spark Adaptive Execution ("reduce-side" partition counts and skew joins)
# MAGIC 2. Structured Streaming Web UI
# MAGIC 
# MAGIC Notable
# MAGIC 1. SQL Documentation
# MAGIC 2. Efficient User-Defined Aggregators
# MAGIC 3. Improved Pandas UDFs
# MAGIC 4. PySpark UDF to convert VectorUDT to Array
# MAGIC 
# MAGIC Overall there are hundreds of changes and bug fixes not listed here -- see the release notes or Spark JIRA for more info.
# MAGIC 
# MAGIC #### Spark 3.1
# MAGIC 
# MAGIC 1. Kubernetes GA support
# MAGIC 2. Improved ANSI SQL support
# MAGIC 3. Node decommissioning and autoscaling (dynamic allocation) improvements
# MAGIC 4. Improved docs (search)
# MAGIC 5. "Project Zen" PySpark improvements
# MAGIC 
# MAGIC #### Spark 3.2
# MAGIC 
# MAGIC 1. Push-based shuffle
# MAGIC 1. Session-state-window, RocksDB state store for Structured Streaming
# MAGIC 1. Koalas integration (Project Zen)

# COMMAND ----------

# MAGIC %md ### Basic Architecture
# MAGIC * Spark cluster / Spark application
# MAGIC   * Driver
# MAGIC   * Executors
# MAGIC   
# MAGIC <img src="http://i.imgur.com/h621Rva.png" width="700px"></img>

# COMMAND ----------

# MAGIC %md #### Spark application(s) vs. Underlying cluster
# MAGIC 
# MAGIC * Spark *applications*, with their drivers and executors, are sometimes referred to as "Spark Clusters"
# MAGIC   * __But__ that "Spark Cluster" is not normally a long-running thing
# MAGIC   * One does not normally "deploy machines (or VMs, or Containers) for a Spark cluster" in the absence of a specific Spark application
# MAGIC   
# MAGIC * The underlying cluster -- most commonly YARN -- may have long-running nodes (hardware, VMs, etc.)
# MAGIC   * *Multiple* Apache Spark application ("Spark clusters") can be launched in that cluster
# MAGIC   * This pattern allows multiple users, teams, departments, etc. to run independent Spark applications on a common, shared infrastructure
# MAGIC 
# MAGIC 
# MAGIC <img src="http://i.imgur.com/vJ55hxW.png" width="800px"></img>

# COMMAND ----------

# MAGIC %md ####Programming Spark
# MAGIC * Several ways
# MAGIC   * Interactive (shell, notebooks)
# MAGIC   * Scripts
# MAGIC   * Compiled Programs
# MAGIC * Languages
# MAGIC   * SQL
# MAGIC   * Scala
# MAGIC   * Python
# MAGIC   * R
# MAGIC   * Java
# MAGIC   * C#, F# (via Mobius)
# MAGIC   * JavaScript (via Eclair)
# MAGIC   * others
# MAGIC   
# MAGIC Principally: Scala, SQL, Python, R

# COMMAND ----------

# MAGIC %md ####Let's Get Some Data!
# MAGIC 
# MAGIC The Wikimedia Project hosts a ton of analytics data -- in addition to their regular content. 
# MAGIC 
# MAGIC Let's take a look at what pages were most popular on a recent day...

# COMMAND ----------

# MAGIC %scala
# MAGIC val ACCESS="AKIAJZEHPW46CWPUWUNQ"
# MAGIC val SECRET="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"
# MAGIC val BUCKET = "cool-data"
# MAGIC val MOUNT = "/mnt/spark-data"
# MAGIC 
# MAGIC try {
# MAGIC   dbutils.fs.mount("s3a://"+ ACCESS + ":" + SECRET + "@" + BUCKET, MOUNT)
# MAGIC } catch {
# MAGIC   case _: Throwable => println("Error mounting ... possibly already mounted")
# MAGIC }

# COMMAND ----------

# MAGIC %md How do we just take a look into a file?

# COMMAND ----------

# MAGIC %scala
# MAGIC spark.read.text("/mnt/spark-data/pageviews.gz").show(false)

# COMMAND ----------

# MAGIC %scala
# MAGIC spark.read.option("delimiter", " ").csv("/mnt/spark-data/pageviews.gz").show

# COMMAND ----------

# MAGIC %md Let's fix the schema. We know (from Wikimedia) that the first three columns are:
# MAGIC   * `project : string`
# MAGIC   * `page : string`
# MAGIC   * `requests : int`	
# MAGIC   * we can ignore the 4th column -- we won't be using it
# MAGIC   
# MAGIC One way to do this is to use `selectExpr` and SQL snippets to `CAST` and rename the columns:

# COMMAND ----------

# MAGIC %scala
# MAGIC val wikimediaData = spark.read
# MAGIC                           .option("delimiter", " ")
# MAGIC                           .csv("/mnt/spark-data/pageviews.gz")
# MAGIC                           .selectExpr("_c0 AS project", "_c1 AS page", "CAST(_c2 AS INT) AS requests")

# COMMAND ----------

# MAGIC %md Let's give this table (really a view, or query) a name ... so that we can look at it with SQL:

# COMMAND ----------

# MAGIC %scala
# MAGIC wikimediaData.createOrReplaceTempView("pageviews")

# COMMAND ----------

# MAGIC %sql SHOW TABLES

# COMMAND ----------

# MAGIC %sql DESCRIBE pageviews;

# COMMAND ----------

# MAGIC %sql SELECT * FROM pageviews WHERE project = 'en' ORDER BY requests DESC;

# COMMAND ----------

# MAGIC %sql SELECT * FROM pageviews WHERE project = 'en' AND page LIKE '%Spark%' ORDER BY requests DESC;

# COMMAND ----------

# MAGIC %md We can write all kinds of SQL queries:
# MAGIC 
# MAGIC * SQL:2003 + HiveQL ... documented at https://spark.apache.org/docs/latest/sql-ref.html
# MAGIC 
# MAGIC ... and we can use a programmatic API or "domain-specific language" as well.
# MAGIC 
# MAGIC The DataFrame/Dataset API allows us to write native Python, Java, Scala, or R programs using Spark.
# MAGIC 
# MAGIC Common tasks are fairly similar across these APIs:
# MAGIC 
# MAGIC |SQL|DataFrame API|DataFrame example (with String column names)|
# MAGIC |---|---|---|
# MAGIC |SELECT|select, selectExpr|myDataFrame.select("someColumn")|
# MAGIC |WHERE|filter, where|myDataFrame.filter("someColumn > 10")|
# MAGIC |GROUP BY|groupBy|myDataFrame.groupBy("someColumn")|
# MAGIC |ORDER BY|orderBy|myDataFrame.orderBy("column")|
# MAGIC |JOIN|join|myDataFrame.join(otherDataFrame, "innerEquiJoinColumn")|
# MAGIC |UNION|union|myDataFrame.union(otherDataFrame)|
# MAGIC 
# MAGIC The API support -- both for SQL and DataFrame/Dataset -- is far broader than these examples. E.g., columns and expressions can be written in a strongly-typed manner, using Column objects. These can be generated from Strings or Symbols; many types of JOIN are supported; and a variety of mechanisms for creating a DataFrame from a source exist in all of these APIs.

# COMMAND ----------

# MAGIC %scala
# MAGIC // Here's the earlier query with the DataFrame/Dataset API:
# MAGIC 
# MAGIC val query = spark.table("pageviews").filter("project = 'en'").filter('page like "%Spark%").orderBy('requests desc)
# MAGIC 
# MAGIC display(query)

# COMMAND ----------

# MAGIC %scala
# MAGIC // Let's take that apart:
# MAGIC 
# MAGIC val query = spark.table("pageviews")

# COMMAND ----------

# MAGIC %scala
# MAGIC display(query)

# COMMAND ----------

# MAGIC %scala
# MAGIC val step1 = spark.table("pageviews")
# MAGIC 
# MAGIC val step2 = step1.filter("project = 'en'")

# COMMAND ----------

# MAGIC %scala
# MAGIC val step1 = spark.table("pageviews")
# MAGIC 
# MAGIC val step2 = step1.filter("project = 'en'")
# MAGIC 
# MAGIC val step3 = step2.filter('page like "%Spark%") // what is 'page here? A Scala symbol that is converted to a column object in context

# COMMAND ----------

# MAGIC %scala
# MAGIC display(step3)

# COMMAND ----------

# MAGIC %scala
# MAGIC //Show me this "page" column object
# MAGIC 
# MAGIC val theDataFrame = spark.table("pageviews")
# MAGIC 
# MAGIC val theColumn = theDataFrame("page")

# COMMAND ----------

# MAGIC %scala
# MAGIC // So what is 'page like "%Spark%"? An API call on Column, that returns another Column:
# MAGIC 
# MAGIC theColumn.like("%Spark%") // Java-style syntax, same call

# COMMAND ----------

# MAGIC %md ####Where do these API docs live?
# MAGIC 
# MAGIC 1. Dataset class (DataFrame is a limited but very useful form of Dataset ... it is a Dataset[Row])
# MAGIC 2. Column
# MAGIC 3. RelationalGroupedDataset (and KeyValueGroupedDataset, a special case we'll see later)
# MAGIC 4. *tons* of helpful functions in org.apache.spark.sql.functions (package object)
# MAGIC   * https://spark.apache.org/docs/latest/api/scala/org/apache/spark/sql/functions$.html
# MAGIC   * also exposed to SQL, pyspark

# COMMAND ----------

# MAGIC %scala
# MAGIC spark.conf.set("spark.sql.adaptive.enabled", false)

# COMMAND ----------

# MAGIC %scala
# MAGIC //One more:
# MAGIC 
# MAGIC display(spark.table("pageviews").groupBy("project").count().orderBy('count desc))

# COMMAND ----------

# MAGIC %md #### Hopefully that seemed reasonable ... 
# MAGIC 
# MAGIC __but__ ... it's taking too long!
# MAGIC * ... turns out our Spark code is fine in terms of API ... but the execution underneath isn't quite right yet.
# MAGIC 
# MAGIC How is that possible? We've barely even done anything yet and something is wrong?
# MAGIC * How would we even know? How can we fix it? 
# MAGIC 
# MAGIC Let's figure it out:

# COMMAND ----------

# MAGIC %md ####Spark is a parallel cluster computing tool...
# MAGIC 
# MAGIC so we're using lots of nodes or at least cores, right?
# MAGIC 
# MAGIC Take a look at the parallelism in that last query. In fact, we're only using 1 core.
# MAGIC 
# MAGIC We'll talk about why, and about how to fix it, but first, let's get some more terminology on the table
# MAGIC * Application
# MAGIC * Query
# MAGIC * Job
# MAGIC * Stage
# MAGIC * Task
# MAGIC 
# MAGIC Now we can at least use the Spark Graphical UI to see that we did all of that work with just one thread!

# COMMAND ----------

# MAGIC %md What was the problem? 
# MAGIC 
# MAGIC Recall the data came from wikimedia as a `gzip` file, and I didn't do any "magic" preparing it for today: I just dropped that .gz into the S3 bucket that we pointed Spark at.
# MAGIC 
# MAGIC Unfortunately, gzip is not a *splittable* compression format. So Spark can't read different pieces of it in parallel using different tasks.
# MAGIC 
# MAGIC Instead we'll convert to plain old uncompressed CSV:

# COMMAND ----------

# MAGIC %scala
# MAGIC dbutils.fs.rm("/FileStore/pageviews-csv", true)
# MAGIC 
# MAGIC // delete any data from the destination folder (in case we run this notebook multiple times)

# COMMAND ----------

# MAGIC %scala
# MAGIC spark.table("pageviews").write.option("header", true).csv("/FileStore/pageviews-csv")

# COMMAND ----------

# MAGIC %fs
# MAGIC 
# MAGIC ls /FileStore/pageviews-csv

# COMMAND ----------

# MAGIC %md That takes a while ... remember we have to process it in one thread

# COMMAND ----------

# MAGIC %scala
# MAGIC spark.table("pageviews").count // USING 1 TASK

# COMMAND ----------

# MAGIC %scala
# MAGIC spark.read.csv("/FileStore/pageviews-csv").count // PARALLEL?

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC Notice:
# MAGIC * 2 Jobs
# MAGIC * Reading is done in parallel 
# MAGIC   * That's the number of *tasks* in the first stage of the main job
# MAGIC   * But it's not really faster in this case
# MAGIC   * How is the parallelism determined?
# MAGIC     * In SparkSQL in 2.0 or later, see https://github.com/apache/spark/blob/master/sql/core/src/main/scala/org/apache/spark/sql/execution/DataSourceScanExec.scala
# MAGIC     * Main number comes from defaultParallelism which is usually the total number of cores available    
# MAGIC * Aggregation is required

# COMMAND ----------

# MAGIC %scala
# MAGIC // Let's add in a filter step and see what happens ...
# MAGIC 
# MAGIC spark.read.option("header", true).csv("/FileStore/pageviews-csv").filter('project==="en").count

# COMMAND ----------

# MAGIC %md Notice that adding a filter took a little longer -- since more computation was happening -- but it did not add any tasks (or stages, jobs, etc.). 
# MAGIC 
# MAGIC This -- inlining of "map" operations into tasks -- is called pipelining and is a key part of the Spark architecture. Tasks read from storage (or an external system, previous shuffle, etc.), compose *all* of the narrow operations on a partition, and then write the data out to storage, shuffle, or the driver. 
# MAGIC 
# MAGIC *So intermediate results never need to be materialized!* (this is key to handling large volumes of data efficiently)

# COMMAND ----------

# MAGIC %md Now we've changed formats, and we're reading a file source (no table name) and using Scala ... but is the CSV version of the data the one we really want?
# MAGIC 
# MAGIC CSV is not the most performant (or compact) format we can use.
# MAGIC 
# MAGIC Other things being equal, parquet is a the default recommended file format. It is a columnar, compressed file format based on the Google's Dremel paper, and it generally provides great performance on Spark, Impala, Presto and other data tools. It even supports partitioning based on the contents, so that queries don't need to process the entire dataset to find the data they are looking for.
# MAGIC 
# MAGIC <img src="https://i.imgur.com/ex7zY3U.png" width=500/>
# MAGIC 
# MAGIC Let's rewrite our data to parquet:

# COMMAND ----------

# MAGIC %scala
# MAGIC dbutils.fs.rm("/FileStore/pageviews", true)
# MAGIC 
# MAGIC // clear out the destination folder in case we run this notebook multiple times

# COMMAND ----------

# MAGIC %scala
# MAGIC spark.table("pageviews").write.parquet("/FileStore/pageviews")

# COMMAND ----------

# MAGIC %md ... and give the new dataset a table name:

# COMMAND ----------

# MAGIC %scala
# MAGIC spark.read.parquet("/FileStore/pageviews").filter('project==="en").createOrReplaceTempView("pv")

# COMMAND ----------

# MAGIC %md Now we can try out all sort of interesting SQL queries...

# COMMAND ----------

# MAGIC %sql SELECT * FROM pv WHERE page = 'New_York'

# COMMAND ----------

# MAGIC %md 
# MAGIC Let's try a simulation of a business use case: we'll join our pageview data with some geography data and see whether big cities get searched more than small ones

# COMMAND ----------

# MAGIC %fs
# MAGIC 
# MAGIC ls /mnt/spark-data

# COMMAND ----------

# MAGIC %scala
# MAGIC spark.read.text("/mnt/spark-data/zips.json").show(false)

# COMMAND ----------

# MAGIC %scala
# MAGIC spark.read.json("/mnt/spark-data/zips.json").withColumnRenamed("_id", "zip").createOrReplaceTempView("zip")

# COMMAND ----------

# MAGIC %sql SELECT * FROM zip

# COMMAND ----------

# MAGIC %sql SELECT page, pop, requests FROM pv JOIN zip ON page = city ORDER BY requests DESC;

# COMMAND ----------

# MAGIC %md That runs ... but ... business logic fail!
# MAGIC * There are multiple postcodes for many of the cities -- and certainly for all of the big ones. We need to aggregate their population.
# MAGIC * We need to normalize the join keys ... the query is case sensitive (e.g., MILAN here is not Milan, NH or even Milan, Italy, but rather the MILAN weapon: https://en.wikipedia.org/wiki/MILAN)
# MAGIC * Multitoken names won't match: New\_York (in Wikipedia) won't match NEW YORK in the zips dataset
# MAGIC 
# MAGIC Let's see if there are some built-in functions that might help:

# COMMAND ----------

# MAGIC %sql SHOW FUNCTIONS

# COMMAND ----------

# MAGIC %md One of the most valuable features of Spark is this set of functions. Look for it in
# MAGIC * org.apache.spark.sql.functions (Scala API docs)
# MAGIC * pyspark.sql.functions (Python API docs)
# MAGIC * or here as SQL functions (SQL API docs)
# MAGIC 
# MAGIC Beyond workhorses like date arithmetic or trig functions, there are some incredibly useful functions that address common use cases and which would be exceptionally difficult to implement yourself. Even better, many of these built-ins are optimized to support compilation and operation on native types, so they're often faster than anything you could write with the public API.

# COMMAND ----------

# MAGIC %sql SELECT split(page, '_') FROM pv WHERE page LIKE 'New_%'

# COMMAND ----------

# MAGIC %md Ok, how about our report of Wikipedia requests for large cities:

# COMMAND ----------

# MAGIC %sql SELECT city, state, SUM(pop), FIRST(requests)
# MAGIC      FROM pv
# MAGIC      JOIN zip ON lower(page) = regexp_replace(lower(city), ' ', '_') 
# MAGIC      GROUP BY city, state 
# MAGIC      ORDER BY FIRST(requests) DESC;
# MAGIC      
# MAGIC -- This produces (likely) spurious extra rows for cities with the same name in multiple states (e.g., Boston, MA vs Boston, TX) -- without more Wikipedia detail we can't disambiguate these here

# COMMAND ----------

# MAGIC %md Something fishy is going on though: look at San Francisco's population ... that's way too high.
# MAGIC 
# MAGIC __Exercise__: Can you find the real population data in the zip table, and figure out what's going wrong?

# COMMAND ----------

# MAGIC %scala
# MAGIC // try it!
