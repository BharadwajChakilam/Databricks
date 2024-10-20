# Databricks notebook source
# MAGIC %run "../includes/configuration"
# MAGIC
# MAGIC

# COMMAND ----------

display(dbutils.fs.ls("abfss://raw@formula1dl1102.dfs.core.windows.net"))

# COMMAND ----------

dbutils.widgets.text("p_file_date","2021-03-21")
v_file_date=dbutils.widgets.get("p_file_date")

# COMMAND ----------

from pyspark.sql.types import StructField,StringType, IntegerType, StringType, DoubleType, StructType, DateType, time
from pyspark.sql.functions import col, current_timestamp, lit, to_timestamp, concat

# COMMAND ----------

schema=StructType([StructField("raceId",IntegerType(), False),
                  StructField("year",IntegerType(), True),
                  StructField("round",IntegerType(), True),
                  StructField("circuitId",IntegerType(), True),
                  StructField("name",StringType(), True),
                  StructField("date",StringType(), True),
                  StructField("time",StringType(), True),
                    StructField("url",StringType(), True)
                  ])

# COMMAND ----------

races_df=spark.read.csv(f"{raw_folder_path}/{v_file_date}/races.csv", schema, header=True)
display(races_df)

# COMMAND ----------

races_df.printSchema()

# COMMAND ----------

display(races_df.describe())

# COMMAND ----------

races_selected_df = races_df.select("raceId", "year", "round", "circuitId", "name", "date", "time")

# COMMAND ----------

display(races_selected_df)

# COMMAND ----------

print(races_selected_df)

# COMMAND ----------

races_column_renamed=races_selected_df.withColumnRenamed("raceId","race_id").withColumnRenamed("year","race_year").withColumnRenamed("circuitId","circuit_id").withColumn("file_date",lit(v_file_date))

# COMMAND ----------

display(races_column_renamed)

# COMMAND ----------

races_prefinal_df=races_column_renamed.withColumn("race_timestamp",to_timestamp(concat(col("date"),lit(' '),col('time')),'yyyy-MM-dd HH:mm:ss'))

# COMMAND ----------

races_dropped=races_prefinal_df.drop("date", "time")

# COMMAND ----------


races_final_df=races_dropped.withColumn("ingestion_date",current_timestamp())

# COMMAND ----------

display(races_final_df)

# COMMAND ----------

races_final_df.printSchema()

# COMMAND ----------


races_final_df.write.partitionBy("race_year").mode("overwrite").format("parquet").saveAsTable("f1_processeds.races")

# COMMAND ----------

# MAGIC %fs
# MAGIC ls
