"""Lab 2, Section 11: a first machine learning pipeline on Spark.

The point of this section is not the model. It is that a pipeline is built from
the same kind of transformations you have used all lab, and runs the same way.
Run:  spark-submit ~/bda/lab02/src/q6_ml.py
"""
from pyspark.sql import SparkSession, functions as F
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression
from pyspark.ml.evaluation import RegressionEvaluator

spark = (SparkSession.builder.appName("lab02-ml").master("local[4]").getOrCreate())
spark.sparkContext.setLogLevel("ERROR")

HDFS = "hdfs://localhost:9000"
q1 = spark.read.parquet(f"{HDFS}/user/tdat1/nyc/curated_q1")

# Keep ordinary trips. A 200 mile taxi ride is real in the data but it is not
# what we are trying to predict, and a handful of them would dominate the error.
data = (q1.select("trip_distance", "trip_minutes", "pickup_hour", "fare_amount")
          .filter((F.col("trip_distance") < 30)
                & (F.col("trip_minutes") < 120)
                & (F.col("fare_amount") > 0)
                & (F.col("fare_amount") < 200)))

train, test = data.randomSplit([0.8, 0.2], seed=42)
print()
print(f"  rows kept for modelling  {data.count():,}")
print(f"  training rows            {train.count():,}")
print(f"  test rows                {test.count():,}")

# A pipeline is a list of stages. Each stage transforms the DataFrame.
assembler = VectorAssembler(
    inputCols=["trip_distance", "trip_minutes", "pickup_hour"],
    outputCol="features")
regressor = LinearRegression(featuresCol="features", labelCol="fare_amount")
pipeline = Pipeline(stages=[assembler, regressor])

model = pipeline.fit(train)
lr = model.stages[-1]

print()
print("  fare_amount  =  {:.3f} * trip_distance".format(lr.coefficients[0]))
print("                + {:.3f} * trip_minutes".format(lr.coefficients[1]))
print("                + {:.3f} * pickup_hour".format(lr.coefficients[2]))
print("                + {:.3f}".format(lr.intercept))

pred = model.transform(test)
for metric in ["rmse", "mae", "r2"]:
    v = RegressionEvaluator(labelCol="fare_amount", predictionCol="prediction",
                            metricName=metric).evaluate(pred)
    print(f"  {metric:>22}   {v:8.3f}")

err = pred.withColumn("error", F.abs(F.col("prediction") - F.col("fare_amount")))

print()
print("  a typical trip, where the model works")
(err.filter((F.col("trip_distance") > 1) & (F.col("trip_distance") < 5))
    .select("trip_distance", F.round("trip_minutes", 1).alias("minutes"),
            "fare_amount", F.round("prediction", 2).alias("predicted"))
    .show(5, truncate=False))

print("  the five trips the model got most wrong")
(err.orderBy(F.desc("error"))
    .select("trip_distance", F.round("trip_minutes", 2).alias("minutes"),
            "fare_amount", F.round("prediction", 2).alias("predicted"),
            F.round("error", 2).alias("error"))
    .show(5, truncate=False))

spark.stop()
