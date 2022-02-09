// Databricks notebook source
// MAGIC %md # Analytics Lab

// COMMAND ----------

// MAGIC %md In this lab, we'll try using Spark to do some analytics on a airline flight dataset.
// MAGIC 
// MAGIC The data is a subset of a large, well-known government transport publication described here: http://www.transtats.bts.gov/Fields.asp?Table_ID=236
// MAGIC 
// MAGIC If we have time at the end, we'll even try an ETL exercise where we load a reshaped subset of the data into MongoDB

// COMMAND ----------

// MAGIC %md The data is located at `dbfs:/databricks-datasets/asa/small`
// MAGIC 
// MAGIC Take a look and determine how to read an initial "raw look" into Spark. Once we see the data, we may want to adjust our approach based on the contents of the file.

// COMMAND ----------

// MAGIC %fs ls /databricks-datasets/asa/small

// COMMAND ----------

spark.read.text("/databricks-datasets/asa/small").take(10).foreach(println)

// COMMAND ----------

// MAGIC %md Based on the appearance of the data, let's drill down a little and load it up with the right formatting/schema

// COMMAND ----------

val raw = spark.read.option("header", true).option("inferSchema", true).csv("/databricks-datasets/asa/small")

// COMMAND ----------

// MAGIC %md Look at the data and the schema

// COMMAND ----------

display(raw)

// COMMAND ----------

raw.printSchema

// COMMAND ----------

// MAGIC %md Does the schema make sense? If not, why not?

// COMMAND ----------

// MAGIC %md Generate summary statistics for each column (hint: use the `describe` method)

// COMMAND ----------

display(raw.describe())

// COMMAND ----------

// MAGIC %md A bunch of columns are missing from the summary ... why is that?
// MAGIC 
// MAGIC Hint: Stats are only calculated for numeric columns -- take a look again at the schema.
// MAGIC 
// MAGIC Can you think of why some columns that appear numeric did not get processed that way by Sparks's CSV schema inference?

// COMMAND ----------

import org.apache.spark.sql.types._

raw.orderBy('ActualElapsedTime).show()

// COMMAND ----------

import org.apache.spark.sql.functions._

raw.filter( isnull('ActualElapsedTime cast IntegerType) ).count

// COMMAND ----------

display(raw.filter('ActualElapsedTime === "NA"))

// COMMAND ----------

raw.filter('ActualElapsedTime === "NA" && 'AirTime === "NA" && 'ArrDelay === "NA").count

// COMMAND ----------

raw.count

// COMMAND ----------

// MAGIC %md It looks like about 1.5% of these records are seriously broken. Find them and continue your analysis with the remaining records.

// COMMAND ----------

val cleanedTemp = raw.filter($"ActualElapsedTime" =!= "NA")


// COMMAND ----------

cleanedTemp.printSchema

// COMMAND ----------

// MAGIC %md Let's write the somewhat cleaner data out to /FileStore/flight-small-clean as a CSV file. Then we'll read it back in and use this file going forward.

// COMMAND ----------

dbutils.fs.rm("/FileStore/flight-small-clean", true)
cleanedTemp.write.option("header", true).csv("/FileStore/flight-small-clean")

// COMMAND ----------

val cleaned = spark.read.option("inferSchema", true).option("header", true).csv("/FileStore/flight-small-clean")

// COMMAND ----------

cleaned.printSchema

// COMMAND ----------

// MAGIC %md Run describe again, verify it works this time, and look at the data summary.

// COMMAND ----------

display(cleaned.describe())

// COMMAND ----------

// MAGIC %md Question 1: What does the distribution of Arrival Delay times look like?
// MAGIC 
// MAGIC Let's do this with SQL. Create a temp view called "flt" and then SELECT the arrival delay data. Make a histogram to visualize it.

// COMMAND ----------

cleaned.createOrReplaceTempView("flt")

// COMMAND ----------

// MAGIC %sql SELECT ArrDelay FROM flt

// COMMAND ----------

// MAGIC %md Question 2: How does the flight duration correlate to that same flight's arrival delay? (Hint: look at the Dataset's `.stat` member)

// COMMAND ----------

cleaned.stat.corr("AirTime", "ArrDelay")

// COMMAND ----------

// MAGIC %md Question 3: How does departure delay correlate to arrival delay? I.e., given that a plane can only "make up so much time" in the air, we would expect a pretty strong pattern where late departing flights also arrive late. On the other hand, for long flights or flights with lots of "padding" built into their schedule, this may not hold true. Let's have Spark calculate this departure delay vs. arrival delay correlation.

// COMMAND ----------

cleaned.stat.corr("DepDelay", "ArrDelay")

// COMMAND ----------

// MAGIC %md Question 4: How do actual arrival times compare to scheduled arrival times? SELECT the difference between these columns and plot it.

// COMMAND ----------

display(cleaned.select('ArrTime - 'CRSArrTime).filter( ('ArrTime - 'CRSArrTime) > -300))

// COMMAND ----------

// MAGIC %md Question 5: Which airlines are represented in this data set?

// COMMAND ----------

// MAGIC %sql SELECT DISTINCT UniqueCarrier FROM flt

// COMMAND ----------

// MAGIC %md What does this suggest about the data set?

// COMMAND ----------

// MAGIC %md Question 6: How do average delay times vary by airline?

// COMMAND ----------

// MAGIC %sql SELECT avg(ArrDelay), UniqueCarrier FROM flt GROUP BY UniqueCarrier

// COMMAND ----------

// MAGIC %md Question 7: Compare average and std dev of delays for Delta flights originating in ATL vs. United flights from ORD vs. American flights from DFW

// COMMAND ----------

// MAGIC %sql SELECT avg(ArrDelay), stddev(ArrDelay), count(1), UniqueCarrier FROM flt 
// MAGIC WHERE (UniqueCarrier='UA' AND Origin='ORD') OR (UniqueCarrier='AA' AND Origin='DFW') OR (UniqueCarrier='DL' AND Origin='ATL')
// MAGIC GROUP BY UniqueCarrier

// COMMAND ----------

// MAGIC %md For the next series of questions, we are going to look at how the late arrival of an aircraft affects a subsequent departure of that same aircraft.
// MAGIC 
// MAGIC First, let's create a query that represents the initial segment (a flight into airport X) and the subsequent outbound segment from the same airport of that same aircraft. (Hint: we can use the TailNum column as one of our criteria to match up the inbound and outbound segments.)

// COMMAND ----------

// MAGIC %sql SELECT * FROM flt AS a JOIN flt as b ON 
// MAGIC (a.year = b.year AND a.month = b.month AND a.dayofmonth = b.dayofmonth AND a.tailnum = b.tailnum AND a.dest = b.origin)
// MAGIC WHERE a.arrtime < b.deptime AND b.deptime - a.arrtime < 150

// COMMAND ----------

// MAGIC %md Let's refine this table a bit, picking out just the origins, departures, arrival times, departure times, arrival delays, departure delays, and the airline. Another hint: on some short-haul routes, the same aircraft may make multiple round trips between the same stations on the same day. To sort these out, you may want to add condition like the outbound departure time is no more than 2.5 hours after the in-bound arrival time.

// COMMAND ----------

// MAGIC %sql SELECT a.origin as origin1, a.deptime as dep1, a.arrtime as arr1, a.dest as dest1,
// MAGIC a.arrdelay, b.depdelay,
// MAGIC b.origin as origin2, b.deptime as dep2, b.arrtime as arr2, b.dest as dest2,
// MAGIC a.uniquecarrier
// MAGIC FROM flt AS a JOIN flt as b ON 
// MAGIC (a.year = b.year AND a.month = b.month AND a.dayofmonth = b.dayofmonth AND a.tailnum = b.tailnum AND a.dest = b.origin)
// MAGIC WHERE a.arrtime < b.deptime AND b.deptime - a.arrtime < 150

// COMMAND ----------

// MAGIC %md Question 8: Can we make another temp view that represents this data, so we can more easily perform queries against it with SQL? Call the new view "pairsByPlane"

// COMMAND ----------

spark.sql("""SELECT a.origin as origin1, a.deptime as dep1, a.arrtime as arr1, a.dest as dest1,
a.arrdelay, b.depdelay,
b.origin as origin2, b.deptime as dep2, b.arrtime as arr2, b.dest as dest2,
a.uniquecarrier, a.tailnum
FROM flt AS a JOIN flt as b ON 
(a.year = b.year AND a.month = b.month AND a.dayofmonth = b.dayofmonth AND a.tailnum = b.tailnum AND a.dest = b.origin)
WHERE a.arrtime < b.deptime AND b.deptime - a.arrtime < 150""").createOrReplaceTempView("pairsbyplane")

// COMMAND ----------

// MAGIC %md Question 9: What is the average ratio of (outbound) departure delay to (inbound) arrival delay, by airline?
// MAGIC 
// MAGIC For these questions, let's focus only on flights where the inbound plane is actually late (i.e., ignore early or on-time arrivals)

// COMMAND ----------

// MAGIC %sql SELECT avg(depdelay/arrdelay), uniquecarrier FROM pairsbyplane WHERE arrdelay > 0 GROUP BY uniquecarrier

// COMMAND ----------

// MAGIC %md Question 10: What if we look at the same metric by airport, rather than carrier?

// COMMAND ----------

// MAGIC %sql SELECT avg(depdelay/arrdelay) as ratio, dest1 FROM pairsbyplane WHERE arrdelay > 0 GROUP BY dest1 order by ratio desc limit 20

// COMMAND ----------

// MAGIC %sql SELECT avg(depdelay/arrdelay) as ratio, dest1 FROM pairsbyplane WHERE arrdelay > 0 GROUP BY dest1 order by ratio limit 20

// COMMAND ----------

// MAGIC %md Since certain airlines concentrate operations in certain hubs, and smaller airports can have less meaningful behavior due to sparser schedules, let's focus on the hub cities. Due to consolidation of airlines, these belong to United, Delta, and American today but were spread over a couple of other airlines at the time of the data set. Look just at: "SFO" , "LAX", "SEA", "DEN", "SLC", "ORD", "MSP", "DFW", "IAH", "EWR", "JFK", "BOS", "ATL", "CLT", "PHL", "MIA", "LGA", "PHX", "IAD", "DCA", "CLE", "CVG", "DTW"

// COMMAND ----------

// MAGIC %sql SELECT * FROM (SELECT avg(depdelay/arrdelay) as ratio, dest1 FROM pairsbyplane WHERE arrdelay > 0 
// MAGIC GROUP BY dest1 order by ratio) WHERE dest1 
// MAGIC IN ("SFO" , "LAX", "SEA", "DEN", "SLC", "ORD", "MSP", "DFW", "IAH", "EWR", "JFK", "BOS", "ATL", "CLT", "PHL", "MIA", "LGA", "PHX", "IAD", "DCA", "CLE", "CVG", "DTW")

// COMMAND ----------

// MAGIC %md Question 11: What is the overall correlation coefficient between the (inbound) arrival delay and (outbound) departure delay?

// COMMAND ----------

spark.table("pairsbyplane").stat.corr("arrdelay", "depdelay")

// COMMAND ----------

// MAGIC %md Question 12: Maybe some airlines deal better with late planes than others? or other factors are at play? Calculate the coefficient just for United, and then just for American

// COMMAND ----------

spark.table("pairsbyplane").filter("uniquecarrier = 'UA'").stat.corr("arrdelay", "depdelay")

// COMMAND ----------

spark.table("pairsbyplane").filter("uniquecarrier = 'AA'").stat.corr("arrdelay", "depdelay")

// COMMAND ----------

// MAGIC %md Maybe one factor in the cost of "turning around" a plane is how big it is -- one hypothesis is that larger planes take longer to unload, clean, and load. (Note that an opposing hypothesis might be that yes, bigger planes take longer, but this is accounted for in the planned ground time and schedule, so this variable would not impact actual departure delays). To figure out the size, we're going to use another dataset located at `/databricks-datasets/asa/planes`

// COMMAND ----------

// MAGIC %md Load this data up, inspect it, and notice that the `type` data (and some othe info) is missing for a bunch of the planes. Read the dataset excluding rows for which the `type` column is null. (Hint: use the isnull function in org.apache.spark.sql.functions)

// COMMAND ----------

import org.apache.spark.sql.functions._

val planes = spark.read.option("header", true).option("inferSchema", true).csv("/databricks-datasets/asa/planes").filter(! isnull('type) )

// COMMAND ----------

planes.show

// COMMAND ----------

// MAGIC %md Question 13: Which distinct models (of aircraft) are represented in this dataset? Use the DSL (instead of SQL) to take a look.

// COMMAND ----------

display(planes.select('model).distinct)

// COMMAND ----------

// MAGIC %md Question 14: Most of the 7XX models are Boeings (like 737, 747, etc.) List the distinct 7XX models that are present.

// COMMAND ----------

planes.select('model).filter("model LIKE '7%'").distinct.show

// COMMAND ----------

// MAGIC %md Exercise 15: There are lots of specific submodels of plane, so that grouping by model will be very hard to interpret. Let's create a user-defined function (UDF) called "planesize" that will convert the model column into a size according to the following business rules:
// MAGIC 
// MAGIC * If the plane model starts with 77, 76, or 74, call it "large"
// MAGIC * *Otherwise* if the model starts with 7 or A3, call it "med"
// MAGIC * *Otherwise* if it starts with EMB, ERJ, or SAAB, call it "small"
// MAGIC * Anything else should return "misc"

// COMMAND ----------

def convert(s:String) = {
  if (s.startsWith("77") || s.startsWith("74") || s.startsWith("76") )
    "large"
  else if (s.startsWith("7") || s.startsWith("A3"))
    "med"
  else if (s.startsWith("EMB") || s.startsWith("ERJ") || s.startsWith("SAAB"))
    "small"
  else
    "misc"
}

val planesize = spark.udf.register("planesize", convert _)

// COMMAND ----------

// MAGIC %md Using your UDF, group the planes by their `planesize` and count them

// COMMAND ----------

display(planes.groupBy(planesize('model)).count)

// COMMAND ----------

// MAGIC %md Exercise 16: To get ready for further analysis, let's enrich our existing `pairsbyplane` table with the new plane and planesize data. Create a new dataframe with all of this info, called `withSize` and register it as a temp view.

// COMMAND ----------

val withSize = spark.table("pairsbyplane").join(planes.select(planesize('model).as("size"), 'model, 'tailnum), "tailnum").filter('size =!= "misc")
display(withSize)

// COMMAND ----------

withSize.createOrReplaceTempView("withSize")

// COMMAND ----------

// MAGIC %md Question 17: How does the departure delay / arrival delay ratio look when we group by carrier and plane size?

// COMMAND ----------

// MAGIC %sql SELECT avg(depdelay/arrdelay) as ratio, size, uniquecarrier FROM withSize 
// MAGIC WHERE arrdelay > 0
// MAGIC GROUP BY size, uniquecarrier order by ratio limit 20

// COMMAND ----------

// MAGIC %md Hmmm... not a lot of the small planes (regionals) showing up. Are there any in our joined data set?

// COMMAND ----------

// MAGIC %sql SELECT * FROM withSize WHERE size = 'small'

// COMMAND ----------

// MAGIC %md Well that's very useful to know and could mean a number of things for our analysis. Let's wrap up with an entertaining question. If this flight set doesn't include regional planes but only "medium sized" (717, 737, etc.) jets and larger ... then the carriers were using those for some pretty short flights.
// MAGIC 
// MAGIC Question 18: What are the 50 shortest-duration flights you could take in one of these jets?

// COMMAND ----------

display(cleaned.join(planes.select(planesize('model).as("size"), 'model, 'tailnum), "tailnum").orderBy('actualElapsedTime).limit(50))
