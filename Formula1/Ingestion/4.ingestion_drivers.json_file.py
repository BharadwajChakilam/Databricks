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

from pyspark.sql.types import StructType, StructField, StringType

name_schema = StructType([
    StructField("forename", StringType(), True),
    StructField("surname", StringType(), True)
])

# COMMAND ----------

schema=StructType([StructField("driverId",IntegerType(), False),
                   StructField("driverRef",StringType(),True),
                    StructField("number",IntegerType(),True),
                  StructField("code",StringType(), True),
                  StructField("dob",DateType(),True),
                  StructField("name",name_schema, True),
                  StructField("nationality",StringType(),True),
                    StructField("url",StringType(), True)
                  ])

# COMMAND ----------

drivers_df=spark.read.json(f"{raw_folder_path}/{v_file_date}/drivers.json",schema)
display(drivers_df)

# COMMAND ----------

drivers_df.printSchema()

# COMMAND ----------

display(drivers_df.describe())

# COMMAND ----------

drivers_selected_df = drivers_df.drop("url")

# COMMAND ----------

display(drivers_selected_df)

# COMMAND ----------

drivers_column_renamed = drivers_selected_df.withColumnRenamed("driverId", "driver_id").withColumnRenamed("driverRef", "driver_ref").withColumn("ingestion_date", current_timestamp()).withColumn("file_date",lit(v_file_date))

# COMMAND ----------

display(drivers_column_renamed)

# COMMAND ----------

drivers_final_df=drivers_column_renamed.withColumn("name", concat(col("name.forename"), lit(" "), col("name.surname")))

# COMMAND ----------

display(drivers_final_df)

# COMMAND ----------

drivers_final_df.printSchema()

# COMMAND ----------

drivers_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processeds.drivers")

# COMMAND ----------

# MAGIC %fs
# MAGIC ls

# COMMAND ----------

dbutils.notebook.exit("Success")
