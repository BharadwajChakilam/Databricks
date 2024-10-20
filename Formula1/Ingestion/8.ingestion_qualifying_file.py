# Databricks notebook source
display(dbutils.fs.ls("abfss://raw@formula1dl1102.dfs.core.windows.net"))

# COMMAND ----------

from pyspark.sql.types import StructField,StringType, IntegerType, StringType, DoubleType, StructType, DateType, time
from pyspark.sql.functions import col, current_timestamp, lit, to_timestamp, concat

# COMMAND ----------

schema=StructType([StructField("qualifyId",IntegerType(),False),
                  StructField("raceId",IntegerType(),False),
                  StructField("driverId",IntegerType(), True),
                    StructField("constructorId",IntegerType(), True),
                  StructField("number",IntegerType(),True),
                  StructField("position",IntegerType(),True),
                  StructField("q1",StringType(), True),
                    StructField("q2",StringType(), True),
                      StructField("q3",StringType(), True)
                  ])

# COMMAND ----------

qualify_df=spark.read.json("abfss://raw@formula1dl1102.dfs.core.windows.net/qualifying/",schema,multiLine=True)
display(qualify_df)

# COMMAND ----------

qualify_df.printSchema()

# COMMAND ----------

display(qualify_df.describe())

# COMMAND ----------

qualify_final_df = qualify_df.withColumnRenamed("qualifyId","qualify_id").withColumnRenamed("driverId", "driver_id").withColumnRenamed("raceId", "race_id").withColumnRenamed("constructorId","constructor_id").withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

display(qualify_final_df)

# COMMAND ----------

qualify_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processeds.qualifying")

# COMMAND ----------

# MAGIC %fs
# MAGIC ls

# COMMAND ----------

df=spark.read.parquet("abfss://processed@formula1dl1102.dfs.core.windows.net/qualifying",header=True)

# COMMAND ----------

display(df)

# COMMAND ----------

dbutils.notebook.exit("Success")
