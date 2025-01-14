# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

processed_folder_path = "abfss://processed@formula1dl1102.dfs.core.windows.net"
races_df = spark.read.parquet(f"{processed_folder_path}/races") \
    .withColumnRenamed("name", "race_name") \
    .withColumnRenamed("race_timestamp", "race_date")
display(races_df)

# COMMAND ----------

circuits_df=spark.read.parquet(f"{processed_folder_path}/circuits").withColumnRenamed("name","circuit_name").withColumnRenamed("location","circuit_location")
display(circuits_df)


# COMMAND ----------

constructors_df=spark.read.parquet(f"{processed_folder_path}/constructors").withColumnRenamed("name","team")

display(constructors_df)

# COMMAND ----------

results_df=spark.read.parquet(f"{processed_folder_path}/results").withColumnRenamed("time","race_time").withColumnRenamed("file_date","result_file_date")

display(results_df)


# COMMAND ----------

driver_df=spark.read.parquet(f"{processed_folder_path}/drivers").withColumnRenamed("name","driver_name").withColumnRenamed("number","driver_number").withColumnRenamed("nationality","driver_nationality")

display(driver_df)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

race_circuits_df=races_df.join(circuits_df, races_df.circuit_id == circuits_df.circuit_id).select(races_df.race_id,races_df.race_year,
        races_df.race_name,
        races_df.race_date,
        circuits_df.circuit_location,)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

final_df = results_df.join(race_circuits_df, results_df.race_id == race_circuits_df.race_id) \
    .join(driver_df, results_df.driver_id == driver_df.driver_id) \
    .join(constructors_df, results_df.constructor_id == constructors_df.constructor_id) \
    .select(races_df.race_year,
        races_df.race_name,
        races_df.race_date,
        circuits_df.circuit_location,
        driver_df.driver_name,
        driver_df.driver_number,
        driver_df.driver_nationality,
        constructors_df.team,
        results_df.grid,
        results_df.fastest_lap,
        results_df.race_time,
        results_df.points,
        results_df.position,
        results_df.results_file_date
    ).withColumn("create_date", current_timestamp())

# COMMAND ----------

display(final_df.filter("race_year=2020 and race_name='Abu Dhabi Grand Prix'").orderBy(final_df.points.desc()))

# COMMAND ----------


final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_presentation.race_results")

# COMMAND ----------

display(spark.read.parquet(f"{presentation_folder_path}/race_results"))

# COMMAND ----------


