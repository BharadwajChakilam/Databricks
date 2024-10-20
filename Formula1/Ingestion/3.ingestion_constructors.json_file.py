# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

display(dbutils.fs.ls("abfss://raw@formula1dl1102.dfs.core.windows.net"))

# COMMAND ----------

dbutils.widgets.text("p_file_date","2021-03-21")
v_file_date=dbutils.widgets.get("p_file_date")

# COMMAND ----------

from pyspark.sql.types import StructField,StringType, IntegerType, StringType, DoubleType, StructType, DateType, time
from pyspark.sql.functions import col, current_timestamp, lit, to_timestamp, concat

# COMMAND ----------

schema=StructType([StructField("constructorId",IntegerType(), True),
                  StructField("constructorRef",StringType(), True),
                  StructField("name",StringType(), True),
                  StructField("nationality",StringType(),True),
                    StructField("url",StringType(), True)
                  ])

# COMMAND ----------

raw_folder_path

# COMMAND ----------

constructors_df= spark.read.json(f"{raw_folder_path}/{v_file_date}/constructors.json", schema)
display(constructors_df)

# COMMAND ----------

constructors_df.printSchema()

# COMMAND ----------

display(constructors_df.describe())

# COMMAND ----------

constructors_selected_df = constructors_df.drop("url")

# COMMAND ----------

display(constructors_selected_df)

# COMMAND ----------

constructors_column_renamed = constructors_selected_df.withColumnRenamed("constructorId", "constructor_id").withColumnRenamed("constructorRef", "constructor_ref").withColumn("file_date",lit(v_file_date))

# COMMAND ----------

display(constructors_column_renamed)

# COMMAND ----------

constructors_final_df = constructors_column_renamed.withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

display(constructors_final_df)

# COMMAND ----------

constructors_final_df.printSchema()

# COMMAND ----------


constructors_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processeds.constructors")

# COMMAND ----------

# MAGIC %fs
# MAGIC ls

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------


