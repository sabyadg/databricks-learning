// Databricks notebook source
// MAGIC %md # More with Dataframes
// MAGIC 
// MAGIC In this notebook, we'll look at a set of optimizations and more advanced mechanisms for working with Spark Dataframes. 
// MAGIC 
// MAGIC Depending on timing, Q&A, etc., we may not cover everything here.
// MAGIC 
// MAGIC We'll start by locating our parquet-format pageviews dataset and assigning it an alias:

// COMMAND ----------

spark.conf.set("spark.sql.adaptive.enabled", false)

spark.read.parquet("/FileStore/pageviews").filter('project==="en").createOrReplaceTempView("pv")

// COMMAND ----------

// MAGIC %sql SELECT * FROM pv WHERE page = 'New_York'

// COMMAND ----------

// MAGIC %md *Advanced Topic:* How would we speed this sort of query up in a RDBMS, or with Hive? Probably with an index.
// MAGIC 
// MAGIC Spark does not support indices, so this flavor of query -- matching a very small fraction of records -- is not where Spark's performance shines in the default configuration.
// MAGIC 
// MAGIC What do you do if you need this sort of functionality? There are a number of options, depending on your problem and architecture, but remember: 
// MAGIC 
// MAGIC Spark is a whole ecosystem, and you're not limited to just code that ships in Apache or from Databricks. E.g., one thing to look at is Intel's __Optimized Analytics Package for Spark Platform__ which adds index support: https://github.com/Intel-bigdata/OAP
// MAGIC 
// MAGIC Also worth checking out are Apache Hudi, which has some indexing support, and (Databricks) Delta Lake, which has index support in the closed/commercial version.
// MAGIC 
// MAGIC Another approach is to add Apache Pinot to your architecture... but once we start moving outside Spark itself, the scope gets very broad :)

// COMMAND ----------

// MAGIC %md Aside from noticing that your app doesn't perform well, what is another way to understand how Spark plans to run your query?
// MAGIC 
// MAGIC We can ask Spark to *EXPLAIN* in SQL or via the API.
// MAGIC 
// MAGIC Here is an example query -- first, showing results; and then explaining the optimizations applied by the Catalyst rule-based optimizer:

// COMMAND ----------

spark.read.parquet("/FileStore/pageviews").filter('project==="en").select('page).filter('page like "Apache%").show

// COMMAND ----------

spark.read.parquet("/FileStore/pageviews").filter('project==="en").select('page).filter('page like "Apache%").explain(true)

// COMMAND ----------

// MAGIC %md *Advanced topic*: Dive deep into supported qualitative optimizations by looking at https://github.com/apache/spark/blob/master/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/optimizer/Optimizer.scala

// COMMAND ----------

// MAGIC %md What about data that we *do* know about and query a lot ... use cases borrowed from the world of analytical databases? 
// MAGIC 
// MAGIC __Spark 2.2: "Cost-based Optimizer Framework"__ picks up where Catalyst's existing optimizations leave off. You can see which features are included in the initial release, along with links to design, discussion, and code at https://issues.apache.org/jira/browse/SPARK-16026
// MAGIC 
// MAGIC As an Apache open-source project, Spark's JIRAs are public, searchable, and provide great insight into the roadmap and evolution of Spark.
// MAGIC 
// MAGIC Let's take a quick look at the cost-based optimizer ("CBO") in action!

// COMMAND ----------

// MAGIC %md __Join reordering__
// MAGIC 
// MAGIC In previous versions of Spark, joins were typically ordered based on their appearance in the query, without regard for the size of the data. This sometimes lead to inefficient naïve join ordering:

// COMMAND ----------

/*
dbutils.fs.rm("/user/hive/warehouse/biglist", true)
dbutils.fs.rm("/user/hive/warehouse/listsquares", true)
dbutils.fs.rm("/user/hive/warehouse/small", true)
*/

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC SET spark.sql.sources.default=parquet;
// MAGIC SET spark.sql.legacy.createHiveTableByDefault=true;
// MAGIC 
// MAGIC DROP TABLE IF EXISTS biglist;
// MAGIC DROP TABLE IF EXISTS listsquares;
// MAGIC DROP TABLE IF EXISTS small;

// COMMAND ----------

spark.range(10000000).write.saveAsTable("biglist")
spark.range(10000000).withColumn("square", 'id * 'id).write.saveAsTable("listsquares")
spark.range(1000).write.saveAsTable("small")

// COMMAND ----------

spark.table("biglist").join(spark.table("listsquares"), "id").join(spark.table("small"), "id").collect

// COMMAND ----------

// MAGIC %md Take a look at the Spark UI ... Look at the SQL tab, then the description link for the most recent query to see the executed plan.
// MAGIC 
// MAGIC Pretty disappointing! Spark did an expensive Sort-Merge join on all 10,000,000 rows ... and only later completed the small inner join to yield 1,000 rows.
// MAGIC 
// MAGIC ---
// MAGIC 
// MAGIC __Ok, let's try this CBO__
// MAGIC 
// MAGIC Setup: for open-source Spark 2.2, we would need to turn on CBO + join reordering (we can skip this on Databricks, since the config is already "on")
// MAGIC 
// MAGIC `%sql SET spark.sql.cbo.enabled = true`
// MAGIC 
// MAGIC `%sql SET spark.sql.cbo.joinReorder.enabled = true`
// MAGIC 
// MAGIC Next, let's compute table-level stats for our tables:

// COMMAND ----------

// MAGIC %sql 
// MAGIC 
// MAGIC ANALYZE TABLE biglist COMPUTE STATISTICS;
// MAGIC ANALYZE TABLE listsquares COMPUTE STATISTICS;
// MAGIC ANALYZE TABLE small COMPUTE STATISTICS

// COMMAND ----------

spark.table("biglist").join(spark.table("listsquares"), "id").join(spark.table("small"), "id").collect

// COMMAND ----------

// MAGIC %md Look at the executed plan for this version ... the join is reordered to avoid the large (10,000,000 row <-> 10,000,000 row) join.
// MAGIC 
// MAGIC Now let's look at one more feature. What if we add a filter condition so that we only keep 100 rows from the `squares` table. In that case, the "original" plan might be better -- join the big table to the tiny list of squares, yielding 100 rows, then join the `small` table.
// MAGIC 
// MAGIC As a baseline, let's observe that Spark doesn't do this optimization, because it doesn't know the distribution of data in the `square` column, so it can't yet estimate how many rows are involved:

// COMMAND ----------

spark.table("biglist").join(spark.table("listsquares").filter('square < 10000), "id").join(spark.table("small"), "id").collect

// COMMAND ----------

// MAGIC %md Note the SQL UI shows the same query plan as before -- adding the `filter` didn't change anything for Spark.
// MAGIC 
// MAGIC Let's have Spark build column-level statistics on that table and try again:

// COMMAND ----------

// MAGIC %sql ANALYZE TABLE listsquares COMPUTE STATISTICS FOR COLUMNS id, square

// COMMAND ----------

spark.table("biglist").join(spark.table("listsquares").filter('square < 10000), "id").join(spark.table("small"), "id").collect

// COMMAND ----------

// MAGIC %md Referring to the SQL UI, we can see that this change resulted in two improvements:
// MAGIC 
// MAGIC * The __join ordering is optimal and takes into account the `filter`__ that keeps just 100 rows of the squarelist table
// MAGIC * With the additional size data, __Spark avoids the Sort-Merge join and performs both joins as Broadcast__ joins 
// MAGIC 
// MAGIC *This is just a brief look at the CBO technology in Spark 2.2. For more details, look at the presentations from Spark Summit SF 2017:*
// MAGIC 
// MAGIC * Cost Based Optimizer in Apache Spark 2.2 (Part 1) - Ron Hu & Sameer Agarwal https://www.youtube.com/watch?v=qS_aS99TjCM
// MAGIC * Cost Based Optimizer in Apache Spark 2.2 (Part 2) - Zhenhua Wang & Wenchen Fan https://www.youtube.com/watch?v=8J-qffTYheE
// MAGIC * Slides accompanying those sessions - https://www.slideshare.net/databricks/costbased-optimizer-in-apache-spark-22
// MAGIC 
// MAGIC __Takeaway: Benchmarking Perf Improvements on TPC-DS__
// MAGIC * 16 queries show speedup > 30%
// MAGIC * Max speedup is 8x
// MAGIC * Geometric mean of speedup is 2.2x

// COMMAND ----------

// MAGIC %md ### Spark Adaptive Execution
// MAGIC 
// MAGIC __What is it and what problem does it solve?__
// MAGIC 
// MAGIC For queries involving a shuffle (e.g., groupby), Spark will default to 200 (`spark.sql.shuffle.partitions`) downstream partitions (hence tasks). This number is configurable but very hard to get right, and -- even if you get it "right" for one batch of data at one point in the query -- it is fixed for the duration of that query. The config and number were added as a stopgap in 2015 and for various reasons a proper fix was not integrated into an official Apache Spark release until 3.0
// MAGIC 
// MAGIC __What does it do?__
// MAGIC 1. Dynamically adjusts the number of partitions downstream of each shuffle
// MAGIC 2. Optimizes "skew joins" (joins where one or more input tables has unbalanced partitions *after* adjusting for the join condition)
// MAGIC *This is a really big deal*
// MAGIC 
// MAGIC __If this is such a big deal, why a 5-minute demo?__
// MAGIC 1. This is really an internals/tuning topic, so not a big focus for a "first steps" class
// MAGIC 2. Although the core algorithm has been tested (Intel release, Alibaba cloud, etc.) for over 2 years, the new implementation is slighlty different and I want some more time to experiment with its impact on real-world business use cases before announcing my conclusions and best practices.
// MAGIC 
// MAGIC __Further detail__
// MAGIC * https://issues.apache.org/jira/browse/SPARK-31412
// MAGIC * https://issues.apache.org/jira/browse/SPARK-23128
// MAGIC * https://docs.google.com/document/d/1mpVjvQZRAkD-Ggy6-hcjXtBPiQoVbZGe3dLnAKgtJ4k/edit
// MAGIC 
// MAGIC With all that said, let's take a quick look!

// COMMAND ----------

spark.read.parquet("/FileStore/pageviews").show

// COMMAND ----------

spark.conf.set("spark.sql.adaptive.enabled", false)

// COMMAND ----------

spark.read.parquet("/FileStore/pageviews").groupBy('project).count.collect

// COMMAND ----------

spark.conf.get("spark.sql.adaptive.enabled")

// COMMAND ----------

spark.conf.set("spark.sql.adaptive.enabled", true)

// COMMAND ----------

spark.read.parquet("/FileStore/pageviews").groupBy('project).count.collect

// COMMAND ----------

spark.read.parquet("/FileStore/pageviews").groupBy('page).count.collect

// COMMAND ----------

// MAGIC %md __See the Spark SQL UI for enhanced query explanation: pre/post Adaptive Execution__ (new in Spark 3.1)

// COMMAND ----------

// MAGIC %md ## UDFs and Dataset
// MAGIC 
// MAGIC ### Integrating Custom Logic

// COMMAND ----------

val isOdd = spark.udf.register("isOdd", (n:Long) => n%2 == 1 )

// COMMAND ----------

val someNumbers = spark.range(20)

someNumbers.select('id, isOdd('id)).show

// COMMAND ----------

// MAGIC %sql SELECT id, isOdd(id) FROM range(10)

// COMMAND ----------

// MAGIC %md *Advanced Topic:* We can also look at rows -- including just parts of rows, or rows with embedded data structures! -- as a Scala type. This feature is called typed Dataset and allows you to leverage nearly all of the performance-enhancing features of Catalyst and Tungsten, while still having access to native Scala types when you need them.
// MAGIC 
// MAGIC Why? Sometimes we need to call our existing custom business logic (perhaps Java) or it's easier to express a query or aggregation in code:

// COMMAND ----------

spark.read.json("/mnt/spark-data/zips.json").withColumnRenamed("_id", "zip").createOrReplaceTempView("zip")

spark.table("zip").show

// COMMAND ----------

case class Zip(city:String) {
  def isSpecial = {
    city contains "BACON" // complex, legacy, regulated, or otherwise special business logic!
  }
}

// COMMAND ----------

spark.table("zip").as[Zip].filter(_.isSpecial).show

// COMMAND ----------

case class Zip(city:String, state:String, zip:String)

// COMMAND ----------

val aggregated = spark.table("zip").as[Zip].groupByKey(z => z.city + "|" + z.state)
                                           .reduceGroups( (z1, z2) => Zip(z1.city, z1.state, z1.zip + " " + z2.zip) )


display(aggregated)

// COMMAND ----------

// MAGIC %md #### What about Python?
// MAGIC 
// MAGIC This particular approach -- "Dataset" -- is Scala-only, and, in the past, mixing Python code with Spark DataFrame code was a major performance anti-pattern.
// MAGIC 
// MAGIC But __Spark 2.3__ introduced a building block for major improvements in Spark + Python interoperability, especially exciting because it opens the door to elegant integrations between Spark and the Python scientific computing, data science, and machine learning stack, including such favorites as SciPy, NumPy, Pandas, TensorFlow, PyTorch, Numba, Scikit-Learn, and more.
// MAGIC 
// MAGIC This new capability takes the form of:
// MAGIC * Vectorized Pandas scalar UDFs
// MAGIC * Vectorized Pandas group-map UDFs (partial aggregations / flatmap groups to any number of rows)
// MAGIC * Can be registered for SQL/Scala/etc. access via regular `spark.udf.register` 
// MAGIC * Integration with Apache Arrow, a columnar in-memory format supporting zero-copy reads (https://arrow.apache.org/)
// MAGIC 
// MAGIC <img src="https://i.imgur.com/DQkDbUH.png" width=900>

// COMMAND ----------

// MAGIC %md __This sounds like a complex integration! The API must be crazy! ... Actually, it's really easy:__

// COMMAND ----------

// MAGIC %python
// MAGIC 
// MAGIC from pyspark.sql.functions import pandas_udf, PandasUDFType
// MAGIC 
// MAGIC df = spark.createDataFrame(
// MAGIC   [(9, 100, 0, 56), (17, 0, 150, 0), (25, 50, 75, 56)], #grams
// MAGIC   ("bacon", "eggs", "sausage", "spam"))
// MAGIC 
// MAGIC df.show()

// COMMAND ----------

// MAGIC %python
// MAGIC 
// MAGIC spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
// MAGIC 
// MAGIC # important business method, or machine learning model!
// MAGIC @pandas_udf("float", PandasUDFType.SCALAR)
// MAGIC def total_calories(bacon, eggs, sausage, spam):
// MAGIC   return 5.41*bacon + 1.96*eggs + 3.01*sausage + 2.8*spam
// MAGIC   
// MAGIC   
// MAGIC # use it like any other function of column(s):
// MAGIC df.withColumn("calories", total_calories(*df.columns)).show()

// COMMAND ----------

// MAGIC %md While the "old" PandasUDF syntax will be supported for a while, Spark 3.0 adds a new syntax based on type hints which is __much__ more flexible and allows many different function signatures, where the inputs can be column/Series; group/Dataframe; iterators; etc.
// MAGIC 
// MAGIC We'll look at converting our previous example, plus two more common scenarios here.

// COMMAND ----------

// MAGIC %python
// MAGIC 
// MAGIC from pandas import Series
// MAGIC from pyspark.sql.types import FloatType
// MAGIC 
// MAGIC def total_calories_func(bacon: Series, eggs: Series, sausage: Series, spam: Series) -> Series:
// MAGIC   return 5.41*bacon + 1.96*eggs + 3.01*sausage + 2.8*spam
// MAGIC 
// MAGIC total_calories = pandas_udf(total_calories_func, returnType=FloatType())
// MAGIC 
// MAGIC df.withColumn("calories", total_calories(*df.columns)).show()

// COMMAND ----------

// MAGIC %md Aggregation in Python: input here is a column/Series, and output is a scalar. *Warning: the whole group must be able to fit in a single Python VM*

// COMMAND ----------

// MAGIC %python
// MAGIC 
// MAGIC from scipy import stats
// MAGIC 
// MAGIC def geom_mean_func(x:Series) -> float:
// MAGIC     return stats.gmean(x)
// MAGIC   
// MAGIC geom_mean = pandas_udf(geom_mean_func, returnType=FloatType())
// MAGIC 
// MAGIC df.select(geom_mean(df.bacon)).show()

// COMMAND ----------

// MAGIC %md Grouping via Spark, then calculating an aggregate over a Pandas Dataframe in Pandas:

// COMMAND ----------

// MAGIC %python
// MAGIC 
// MAGIC import pandas as pd
// MAGIC 
// MAGIC def avg_mass(pdf):
// MAGIC   pdf['avg_mass'] = sum([sum(pdf[c]) for c in pdf.columns]) / len(pdf)
// MAGIC   return pdf
// MAGIC   
// MAGIC df.groupby(df.spam).applyInPandas(avg_mass, schema="bacon double, eggs double, sausage double, spam double, avg_mass double").show()

// COMMAND ----------

// MAGIC %md That's a bit of a silly example, but we could also be ...
// MAGIC 
// MAGIC * collecting characteristic statistics by group
// MAGIC * building models
// MAGIC * deskewing based on their within-group distribution
// MAGIC 
// MAGIC or anything else!

// COMMAND ----------

// MAGIC %md ### Where to learn more?
// MAGIC 
// MAGIC There's lots of material out there -- make sure you're learning about __modern Spark__ and modern best practices!
// MAGIC 
// MAGIC <table style="border:none"><tr><td style="border:none">
// MAGIC <img src="https://materials.s3.amazonaws.com/i/ls2e.jpg" width=300>
// MAGIC <img src="https://i.imgur.com/LwLmsvX.png" width=300>
// MAGIC <img src="https://i.imgur.com/AkmQ5az.png" width=300>
// MAGIC </td></tr></table>

// COMMAND ----------


