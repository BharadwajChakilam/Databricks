# Databricks notebook source
dbutils.widgets.text("p_data_source","")
v_data_source=dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date","2021-03-21")
v_file_date=dbutils.widgets.get("p_file_date")

# COMMAND ----------

v_file_date

# COMMAND ----------

v_data_source

# COMMAND ----------

# MAGIC %run "../includes/configuration"
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %run "../includes/common_function"

# COMMAND ----------

raw_folder_path

# COMMAND ----------

display(dbutils.fs.ls(raw_folder_path))

# COMMAND ----------

from pyspark.sql.types import StructField,StringType, IntegerType, StringType, DoubleType, StructType  
from pyspark.sql.functions import col, current_timestamp, lit

# COMMAND ----------

schema=StructType([StructField("circuitId",IntegerType(), False),
                  StructField("circuitRef",StringType(), True),
                  StructField("name",StringType(), True),
                  StructField("location",StringType(), True),
                  StructField("country",StringType(), True),
                  StructField("lat",DoubleType(), True),
                  StructField("lng",DoubleType(), True),
                   StructField("alt",IntegerType(), True),
                    StructField("url",StringType(), True)
                  ])

# COMMAND ----------

circuits_df = spark.read.csv(f"{raw_folder_path}/{v_file_date}/circuits.csv", schema, header=True)
display(circuits_df)

# COMMAND ----------

circuits_df.printSchema()

# COMMAND ----------

display(circuits_df.describe())

# COMMAND ----------

circuits_seleccted_df=circuits_df.select(circuits_df.circuitId,circuits_df.circuitRef,circuits_df.name,circuits_df.location,circuits_df.country,circuits_df.lat,circuits_df.lng,circuits_df.alt)

# COMMAND ----------

display(circuits_seleccted_df)

# COMMAND ----------

circuits_seleccted_df=circuits_df.select(circuits_df["circuitId"],circuits_df["circuitRef"],circuits_df["name"],circuits_df["location"],circuits_df["country"],circuits_df["lat"],circuits_df["lng"],circuits_df["alt"])

# COMMAND ----------

display(circuits_seleccted_df)

# COMMAND ----------

from pyspark.sql.functions import col

circuits_selected_df = circuits_df.select(col("circuitId"), col("circuitRef"), col("name"), col("location"), col("country"), col("lat"), col("lng"), col("alt"))

# COMMAND ----------

display(circuits_selected_df)

# COMMAND ----------

print(circuits_selected_df)

# COMMAND ----------

circuits_column_renamed=circuits_selected_df.withColumnRenamed("lat","lattitude").withColumnRenamed("lng","longitude").withColumnRenamed("circuitId","circuit_id").withColumnRenamed("alt","altitude").withColumnRenamed("circuitRef","circuit_ref").withColumn("data_source",lit(v_data_source)).withColumn("file_date",lit(v_file_date))

# COMMAND ----------

display(circuits_column_renamed)

# COMMAND ----------


circuits_final_df=add_ingestion_date(circuits_column_renamed)

# COMMAND ----------

display(circuits_final_df)

# COMMAND ----------

circuits_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processeds.circuits")

# COMMAND ----------

# MAGIC %fs
# MAGIC ls

# COMMAND ----------

processed_folder_path

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------




# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_processeds.circuits;

# COMMAND ----------


