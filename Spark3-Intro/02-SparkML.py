# Databricks notebook source
# MAGIC %md ##Machine Learning with Spark 3
# MAGIC 
# MAGIC *Note: this notebook has Python as its default language, and can be exported as a Jupyter notebook*

# COMMAND ----------

path = "/databricks-datasets/Rdatasets/data-001/csv/ggplot2/diamonds.csv"

spark.read.csv(path).show()

# COMMAND ----------

# MAGIC %md We can use the header data as column names through the regular reader option call:

# COMMAND ----------

spark.read.option("header", True).csv(path).show()

# COMMAND ----------

# MAGIC %md In this example, we're just going to look at carat weight as a linear predictor of price. So we'll grab those columns and use the inferSchema option to get numbers (recall the original data is a CSV file which defaults to reading strings):

# COMMAND ----------

data = spark.read.option("header", True) \
            .option("inferSchema", True) \
            .csv(path) \
            .select("carat", "price")

# COMMAND ----------

# MAGIC %md Let's have a quick look to see if there's any hope for our linear regression:

# COMMAND ----------

display(data.sample(False, 0.01))

# COMMAND ----------

# MAGIC %md What's the Pearson correlation?

# COMMAND ----------

data.stat.corr("price", "carat")

# COMMAND ----------

# MAGIC %md Ok, so there is a chance we'll get something useful out... 
# MAGIC 
# MAGIC But what do we need to feed in?
# MAGIC 
# MAGIC __Feature Vectors!__
# MAGIC 
# MAGIC The training (and validation, test, etc.) data needs to be collected into a column of `Vector[Double]`
# MAGIC 
# MAGIC We could do this conversion ourselves, by writing a UDF, but luckily, there's a built-in `Transformer` that will take one or more columns of numbers and collect them into a new column containing a `Vector[Double]`

# COMMAND ----------

from pyspark.ml.feature import *

assembler = VectorAssembler(inputCols=["carat"], outputCol="features")

# COMMAND ----------

# MAGIC %md First look at the next cell, to help make it concrete what this transformer does, and what transformers do in general.
# MAGIC 
# MAGIC Next, take a look at the VectorAssembler docs and maybe even the source code.

# COMMAND ----------

assembler.transform(data).show()

# COMMAND ----------

# MAGIC %md Now we'll add the `LinearRegression` algorithm. The algorithm builds a model from data.
# MAGIC 
# MAGIC Since it needs to look at all the data and then build a new piece of state (representing the `Model`) that can be used for predictions on each row (a Model is a subtype of `Transformer`), it is an `Estimator`.

# COMMAND ----------

from pyspark.ml.regression import *

lr = LinearRegression(labelCol="price")

# COMMAND ----------

# MAGIC %md We can operate each of these components separately, to see how they're working (but we'll see a shortcut in just a minute)

# COMMAND ----------

train, test = data.randomSplit([0.75, 0.25])

lrModel = lr.fit ( assembler.transform(train) )

# COMMAND ----------

# MAGIC %md How do we make predictions on a new dataset, like our test dataframe?

# COMMAND ----------

lrModel.transform( assembler.transform(test) ).show()

# COMMAND ----------

# MAGIC %md Let's package the processing steps together so that we don't need to run them
# MAGIC * separately
# MAGIC * for training, validation sets, etc.
# MAGIC 
# MAGIC The `Pipeline` is an `Estimator` that represents composing a series of `Transformer`s or `Estimator`-`Model` pairs.
# MAGIC 
# MAGIC When we add an `Estimator` to a pipeline (without specifically fitting the `Estimator` first), we are performing composition -- `Pipeline`s are themselves `Estimator`s, so we're making a new `Estimator` that includes the `LinearRegression` algorithm as a component part.

# COMMAND ----------

from pyspark.ml import Pipeline

pipeline = Pipeline(stages=[assembler, lr])

# COMMAND ----------

model = pipeline.fit(train)

# COMMAND ----------

# MAGIC %md Note this is a summary of the model, not a summary of the test. So it's showing training error, or "apparent error."

# COMMAND ----------

summary = model.stages[-1].summary
print(summary.r2)
print(summary.rootMeanSquaredError)

# COMMAND ----------

display(summary.residuals.sample(False, 0.05)) # training residuals

# COMMAND ----------

# MAGIC %md Now ... how what was the performance on the test data?

# COMMAND ----------

predictions = model.transform(test)

# COMMAND ----------

display(predictions.sample(False, 0.02).selectExpr("prediction - price as error"))

# COMMAND ----------

from pyspark.ml.evaluation import *

eval = RegressionEvaluator(labelCol="price", predictionCol="prediction")

# COMMAND ----------

eval.evaluate(predictions)

# COMMAND ----------

for line in eval.explainParams().split("\n"):
  print(line)

# COMMAND ----------

help(eval)

# COMMAND ----------

# MAGIC %md It looks like we did about as well (or badly) on the test data as on the training data.

# COMMAND ----------

# MAGIC %md Recap... What have we looked and not looked at?
# MAGIC 
# MAGIC Looked at:
# MAGIC * Basic data preparation (types, DataFrame, vectors)
# MAGIC * Feature pre-processing helper example: VectorAssembler
# MAGIC * Role and type of a Transformer
# MAGIC * Model-building algorithm example: LinearRegression
# MAGIC * Role and type of an Estimator
# MAGIC * Pipeline
# MAGIC * Some basic graphs and statistics along the way
# MAGIC 
# MAGIC Have *not* looked at:
# MAGIC * Cleaning, deskewing, other data pre-processing
# MAGIC * Various data prep helpers
# MAGIC * Other algorithms
# MAGIC * Model tuning and cross-validation
# MAGIC * Combining Spark with other models and tools (sklearn, deep learning, etc.)
# MAGIC * Data-parallelism strategy

# COMMAND ----------

# MAGIC %md One-variable linear regression? Really? Can't we do something a tiny bit fancier?
# MAGIC 
# MAGIC For the data scientists, we'll take a quick look at a more powerful model -- a gradient-boosted tree ensemble -- using all of the features in the dataset:

# COMMAND ----------

categoricalFields = ["cut", "color", "clarity"]
indexedCatNames = [f + "Index" for f in categoricalFields]

indexer = StringIndexer(inputCols=categoricalFields, outputCols=indexedCatNames)

assembler = VectorAssembler( \
  inputCols=indexedCatNames + ["carat", "depth", "table", "x", "y", "z"], \
  outputCol="features")

gbt = GBTRegressor(labelCol="price")
gbtPipeline = Pipeline(stages=[indexer, assembler, gbt])

allFields = spark.read.option("header", True).option("inferSchema", True).csv(path)
train, test = allFields.randomSplit([0.75, 0.25])

gbtModel = gbtPipeline.fit(train)
predictions = gbtModel.transform(test)
eval.evaluate(predictions)

# COMMAND ----------

# MAGIC %md ### Major Changes in 3.x
# MAGIC 
# MAGIC -   `OneHotEncoder` which is deprecated in 2.3, is removed in 3.0 and `OneHotEncoderEstimator` is now renamed to `OneHotEncoder`.
# MAGIC -   `org.apache.spark.ml.image.ImageSchema.readImages` which is deprecated in 2.3, is removed in 3.0, use `spark.read.format('image')` instead.
# MAGIC - Project Zen Python docs/usability improvements
# MAGIC   - e.g., http://spark.apache.org/docs/latest/api/python/reference/api/pyspark.ml.feature.UnivariateFeatureSelector.html

# COMMAND ----------

# MAGIC %md # Deep Learning, Python, and Moving Beyond Spark
# MAGIC * The Challenges
# MAGIC   * JVM-based data engineering is not in the core skill set of most data scientists, nor need it be
# MAGIC   * Many large companies cannot deploy a suitably interactive Spark environment and sufficient data eng support
# MAGIC   * You likely don't need big data for high-bias models like linear/logistic regression
# MAGIC   * For high-capacity models like deep neural nets (e.g., TensorFlow) or GBT (e.g., XGBoost) the leading scale-out approaches *don't need Spark and, in fact, work around Spark rather than with it*
# MAGIC   * Spark, while considering GPU-accelerated approaches, would have a long way to go to match the perf and usability of TF, PyTorch, RAPIDS, etc. for dist ML
# MAGIC   * Spark open-source community is looking limited...
# MAGIC     * Hortonworks merged into Cloudera, MapR acquired by HP, Cloudera not profitable
# MAGIC     * Much of the top Spark talent from these other firms has joined Databricks...
# MAGIC     * Which could be good, but Databricks has a delicate balance to maintain as a private software company whose employees build a Spark competitor
# MAGIC       * https://databricks.com/blog/2020/06/24/introducing-delta-engine.html
# MAGIC       * https://databricks.com/product/delta-engine
# MAGIC   
# MAGIC * And yet...
# MAGIC   * The data engineering world in large companies is slow to turn and will rely on JVM/Hadoop/Spark for a long time
# MAGIC   * The Python world does not have a native distributed high-perf SQL engine
# MAGIC   * So to start with SQL queries over data lakes or Hive tables we need Apache Spark, PrestoDB, Apache Ignite, or similar
# MAGIC     * (keep an eye on BlazingSQL! but it's not quite there today and requires CUDA 6.1 GPUs)

# COMMAND ----------

# MAGIC %md ### What About Fast Prediction on Just a Few Vectors?
# MAGIC 
# MAGIC This problem -- fast inference with(out) Spark -- has a long and challenging history, because Spark itself is architected to serve that use case.
# MAGIC 
# MAGIC However, the hard times are behind us, and we can now recommend a straightforward, architecturally clean solution. In a nutshell, it looks like this:
# MAGIC 1. Train your Spark pipeline
# MAGIC 2. Export your Spark pipeline as a ONNX model
# MAGIC 3. Perform inference using the ONNX runtime of your choice, on a hardware+software platform of your choice.
# MAGIC 
# MAGIC #### The Details
# MAGIC 
# MAGIC __ONNX (Open Neural Network eXchange)__
# MAGIC 
# MAGIC Originally created by Facebook and Microsoft as an industry collaboration for import/export of neural networks, it has grown to include support for "traditional" ML models, interop with many software libraries, and has both software (CPU + GPU accelerated) and hardware (Intel, Qualcomm, etc.) runtimes.
# MAGIC 
# MAGIC https://onnx.ai/
# MAGIC 
# MAGIC * Created by Facebook and Microsoft; AWS and many other firms now on board
# MAGIC * Built-in operators, data types with consistent behavior
# MAGIC * Extensible -- e.g., ONNX-ML
# MAGIC 
# MAGIC <img src="https://materials.s3.amazonaws.com/i/9byVguG.png" width=500>
# MAGIC 
# MAGIC Key Points:
# MAGIC * In Q1-Q2 of 2019, Microsoft added a Spark ML Pipeline exporter to the `onnxmltools` project
# MAGIC   * https://github.com/onnx/onnxmltools
# MAGIC * Microsoft open-sourced (Dec 2018) a high-perf runtime (GPU, CPU, language bindings, etc.) https://azure.microsoft.com/en-us/blog/onnx-runtime-is-now-open-source/
# MAGIC   * Being used as part of Windows ML / Azure ML
# MAGIC   * https://github.com/Microsoft/onnxruntime
# MAGIC * Many other ML libraries, including the major deep learning tools, have ONNX support
# MAGIC * MIT license makes it both OSS and business friendly
# MAGIC * The closest thing we have to an open, versatile, next-gen format *with wide support*
# MAGIC * Model itself is stored in a compact and typesafe Protobuf-based format
# MAGIC  *Of the "standard/open" formats, ONNX clearly has the most momentum in the past year or two.*
# MAGIC 
# MAGIC #### How to get started exporting SparkML to ONNX?
# MAGIC 
# MAGIC Look at https://github.com/onnx/onnxmltools/tree/master/onnxmltools/convert/sparkml

# COMMAND ----------


