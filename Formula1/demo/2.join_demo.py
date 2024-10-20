# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

races_df=spark.read.parquet(f"{processed_folder_path}/races").withColumnRenamed("name","races_name").withColumnRenamed("race_timestamp","race_date")

circuits_df=spark.read.parquet(f"{processed_folder_path}/circuits").withColumnRenamed("name","circuit_name").withColumnRenamed("location","circuit_location")

constructors_df=spark.read.parquet(f"{processed_folder_path}/constructors").withColumnRenamed("name","team")

results_df=spark.read.parquet(f"{processed_folder_path}/results").withColumnRenamed("time","race_time")

driver_df=spark.read.parquet(f"{processed_folder_path}/drivers").withColumnRenamed("name","driver_name").withColumnRenamed("number","driver_number").withColumnRenamed("nationality","driver_nationality")

# COMMAND ----------

display(races_df)

# COMMAND ----------

display(circuits_df)

# COMMAND ----------

display(
    circuits_df.join(races_df, races_df.circuit_id == circuits_df.circuit_id,"inner")
    .select(
        circuits_df.circuit_name,
        circuits_df.location,
        circuits_df.country,
        races_df.races_name,
        races_df.round
    )
)

# COMMAND ----------


display(
    circuits_df.join(races_df, races_df.circuit_id == circuits_df.circuit_id,"left")
    .select(
        circuits_df.circuit_name,
        circuits_df.location,
        circuits_df.country,
        races_df.races_name,
        races_df.round
    )
)

# COMMAND ----------

display(
    circuits_df.join(races_df, races_df.circuit_id == circuits_df.circuit_id,"right")
    .select(
        circuits_df.circuit_name,
        circuits_df.location,
        circuits_df.country,
        races_df.races_name,
        races_df.round
    )
)

# COMMAND ----------

display(
    circuits_df.join(races_df, races_df.circuit_id == circuits_df.circuit_id,"full")
    .select(
        circuits_df.circuit_name,
        circuits_df.location,
        circuits_df.country,
        races_df.races_name,
        races_df.round
    )
)

# COMMAND ----------

display(
    circuits_df.join(races_df, races_df.circuit_id == circuits_df.circuit_id,"semi"))

# COMMAND ----------

display(
    circuits_df.join(races_df, races_df.circuit_id == circuits_df.circuit_id,"anti"))

# COMMAND ----------

display(races_df.crossJoin(circuits_df))

# COMMAND ----------

from pyspark.sql.functions import current_timestamp
display(
    circuits_df.join(races_df, races_df.circuit_id == circuits_df.circuit_id),
     results_df.join(races_df, races_df.race_id == results_df.race_id),
    results_df.join(driver_df, driver_df.driver_id == results_df.driver_id),
results_df.join(constructors_df, constructors_df.constructor_id == results_df.constructor_id),
    ).select(races_df.race_year,races_df.race_name,races_df.race_date,circuits_df.circuit_location,driver_df.driver_name,driver_df.driver_number,driver_df.driver_nationality,constructors_df.team,results_df.grid,results_df.fastest_lap,results_df.race_time,results_df.points).withcolumn("create_date",current_timestamp)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# Perform the joins sequentially and store the result in a single DataFrame
final_df = circuits_df.join(races_df, races_df.circuit_id == circuits_df.circuit_id) \
    .join(results_df, races_df.race_id == results_df.race_id) \
    .join(driver_df, driver_df.driver_id == results_df.driver_id) \
    .join(constructors_df, constructors_df.constructor_id == results_df.constructor_id) \
    .select(
        races_df.race_year,
        races_df.races_name,
        circuits_df.location,
        driver_df.driver_name,
        driver_df.driver_number,
        driver_df.driver_nationality,
        constructors_df.team,
        results_df.grid,
        results_df.fastest_lap,
        results_df.race_time,
        results_df.points
    ) \
    .withColumn("create_date", current_timestamp())

# Display the final DataFrame
display(final_df)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# Perform the joins sequentially and store the result in a single DataFrame
final_df = circuits_df.join(races_df, races_df.circuit_id == circuits_df.circuit_id) \
    .join(results_df, races_df.race_id == results_df.race_id) \
    .join(driver_df, driver_df.driver_id == results_df.driver_id) \
    .join(constructors_df, constructors_df.constructor_id == results_df.constructor_id) \
    .select(
        races_df.race_year,
        races_df.races_name,
        races_df.race_date,  # Include race_date column
        circuits_df.circuit_location,
        driver_df.driver_name,
        driver_df.driver_number,
        driver_df.driver_nationality,
        constructors_df.team,
        results_df.grid,
        results_df.fastest_lap,
        results_df.race_time,
        results_df.points
    ) \
    .withColumn("create_date", current_timestamp())

# Display the final DataFrame
display(final_df)

# COMMAND ----------


