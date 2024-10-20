# Databricks notebook source
display(dbutils.fs.ls("abfss://raw@formula1dl1102.dfs.core.windows.net"))

# COMMAND ----------

from pyspark.sql.types import StructField,StringType, IntegerType, StringType, DoubleType, StructType, DateType, time
from pyspark.sql.functions import col, current_timestamp, lit, to_timestamp, concat

# COMMAND ----------

schema=StructType([StructField("raceId",IntegerType(),False),
                  StructField("driverId",IntegerType(), True),
                    
                  StructField("stop",StringType(), True),
                  StructField("lap",StringType(),True),
                  StructField("time",StringType(), True),
                  StructField("duration",StringType(),True),
                    StructField("milliseconds",IntegerType(), True)
                  ])

# COMMAND ----------

pit_stops_df=spark.read.json("abfss://raw@formula1dl1102.dfs.core.windows.net/pit_stops.json",schema,multiLine=True)
display(pit_stops_df)

# COMMAND ----------

pit_stops_df.printSchema()

# COMMAND ----------

display(pit_stops_df.describe())

# COMMAND ----------

pit_stops_selected_df = pit_stops_df.drop("url")

# COMMAND ----------

display(pit_stops_selected_df)

# COMMAND ----------

pit_stops_final_df = pit_stops_selected_df.withColumnRenamed("driverId", "driver_id").withColumnRenamed("raceId", "race_id").withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

display(pit_stops_final_df)

# COMMAND ----------


pit_stops_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processeds.pit_stops")

# COMMAND ----------

# MAGIC %fs
# MAGIC ls

# COMMAND ----------

df=spark.read.parquet("abfss://processed@formula1dl1102.dfs.core.windows.net/pit_stops",header=True)

# COMMAND ----------

display(df)

# COMMAND ----------

dbutils.notebook.exit("Success")
