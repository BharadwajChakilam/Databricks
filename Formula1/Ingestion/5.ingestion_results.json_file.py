# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

spark.read.json(f"{raw_folder_path}/{'2021-03-21'}/results.json").createOrReplaceTempView("results_cutover")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT raceId, count(1)  FROM results_cutover
# MAGIC GROUP BY raceId
# MAGIC ORDER BY raceId DESC

# COMMAND ----------

spark.read.json(f"{raw_folder_path}/{'2021-04-18'}/results.json").createOrReplaceTempView("results_w2")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT raceId, count(1)  FROM results_w2
# MAGIC GROUP BY raceId
# MAGIC ORDER BY raceId DESC

# COMMAND ----------

dbutils.widgets.text("p_file_date","2021-03-28")
v_file_date=dbutils.widgets.get("p_file_date")

# COMMAND ----------

display(dbutils.fs.ls("abfss://raw@formula1dl1102.dfs.core.windows.net"))

# COMMAND ----------

from pyspark.sql.types import StructField,StringType, IntegerType, StringType, DoubleType, StructType, DateType, time, FloatType
from pyspark.sql.functions import col, current_timestamp, lit, to_timestamp, concat

# COMMAND ----------

schema=StructType([StructField("resultId",IntegerType(), False),
                   StructField("raceId",IntegerType(), False),
                   StructField("driverId",IntegerType(), False),
                   StructField("constructorId",IntegerType(), False),
                   StructField("number",IntegerType(),True),
                   StructField("grid",IntegerType(), False),
                   StructField("position",IntegerType(), True),
                   StructField("positionText",StringType(), False),
                   StructField("positionOrder",IntegerType(), False),
                   StructField("points",FloatType(), False),
                  StructField("laps",IntegerType(), False),
                   StructField("time",StringType(),True),
                   StructField("milliseconds",IntegerType(),True),
                     StructField("fastestLap",IntegerType(), True),
                  StructField("rank",IntegerType(), True),
                  StructField("fastestLapTime",StringType(),True),
                  StructField("fastestLapSpeed",StringType(),True),
                    StructField("statusId",IntegerType(), False)
                  ])

# COMMAND ----------

results_df=spark.read.json(f"{raw_folder_path}/{v_file_date}/results.json")
display(results_df)

# COMMAND ----------

results_df.printSchema()

# COMMAND ----------

display(results_df.describe())

# COMMAND ----------

results_selected_df = results_df.drop("statusId")

# COMMAND ----------

display(results_selected_df)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

results_column_renamed = (results_selected_df
    .withColumnRenamed("resultId","result_id")
    .withColumnRenamed("raceId","race_id")
    .withColumnRenamed("positionText","position_text")
    .withColumnRenamed("driverId", "driver_id")
    .withColumnRenamed("constructorId", "constructor_id")
    .withColumnRenamed("positionOrder","position_order")
    .withColumnRenamed("fastestLap","fastest_lap")
    .withColumnRenamed("fastestLapTime","fastest_lap_time")
    .withColumnRenamed("fastestLapSpeed","fastest_lap_speed")
    .withColumn("ingestion_date", current_timestamp())
    .withColumn("file_date", lit(v_file_date))
)

# COMMAND ----------

display(results_column_renamed)

# COMMAND ----------

results_final_df=results_column_renamed

# COMMAND ----------

display(results_final_df)

# COMMAND ----------

results_final_df.printSchema()

# COMMAND ----------

from pyspark.sql.functions import col

# Assuming 'points' should be of type BIGINT as per the table schema, cast it accordingly
results_final_df = results_final_df.withColumn("points", col("points").cast("BIGINT"))

# Cast the 'number' column to BIGINT (as you already did, assuming this matches the table schema)
results_final_df = results_final_df.withColumn("number", col("number").cast("BIGINT"))

# Cast the 'rank' column to BIGINT (assuming this matches the table schema)
results_final_df = results_final_df.withColumn("rank", col("rank").cast("BIGINT"))

# Ensure all other columns match the schema of the existing table in both type and name
results_final_df = results_final_df.select(
    "constructor_id",
    "driver_id",
    "fastest_lap",
    "fastest_lap_speed",
    "fastest_lap_time",
    "grid",
    "laps",
    "milliseconds",
    "number",
    "points",
    "position",
    "position_order",
    "position_text",
    "race_id",
    "rank",
    "result_id",
    "time",
    "ingestion_date",
    "file_date"
)

# Now write the DataFrame to the table, without adding 'rank_new'
results_final_df.write.partitionBy("race_id") \
    .mode("append") \
    .format("parquet") \
    .saveAsTable("f1_processeds.results")

# COMMAND ----------

# MAGIC %fs
# MAGIC ls

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------


