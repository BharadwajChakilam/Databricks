# Databricks notebook source
display(dbutils.fs.ls("abfss://raw@formula1dl1102.dfs.core.windows.net"))

# COMMAND ----------

from pyspark.sql.types import StructField,StringType, IntegerType, StringType, DoubleType, StructType, DateType, time
from pyspark.sql.functions import col, current_timestamp, lit, to_timestamp, concat

# COMMAND ----------

schema=StructType([StructField("raceId",IntegerType(),False),
                  StructField("driverId",IntegerType(), True),
                  StructField("lap",StringType(),True),
                  StructField("position",IntegerType(),True),
                  StructField("time",StringType(), True),
                    StructField("milliseconds",IntegerType(), True)
                  ])

# COMMAND ----------

lap_times_df=spark.read.csv("abfss://raw@formula1dl1102.dfs.core.windows.net/lap_times/",schema)
display(lap_times_df)

# COMMAND ----------

lap_times_df.printSchema()

# COMMAND ----------

display(lap_times_df.describe())

# COMMAND ----------

lap_time_final_df = lap_times_df.withColumnRenamed("driverId", "driver_id").withColumnRenamed("raceId", "race_id").withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

display(lap_time_final_df)

# COMMAND ----------

lap_time_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processeds.lap_times")

# COMMAND ----------

# MAGIC %fs
# MAGIC ls

# COMMAND ----------

df=spark.read.parquet("abfss://processed@formula1dl1102.dfs.core.windows.net/lap_times",header=True)

# COMMAND ----------

display(df)

# COMMAND ----------

dbutils.notebook.exit("Success")
