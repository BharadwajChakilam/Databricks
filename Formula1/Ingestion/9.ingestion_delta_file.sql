-- Databricks notebook source
-- MAGIC %run "../includes/configuration"

-- COMMAND ----------

-- MAGIC %python
-- MAGIC demo_folder_path

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS f1_demo
LOCATION 'abfss://demo@formula1dl1102.dfs.core.windows.net/f1_demo'

-- COMMAND ----------

-- MAGIC %python
-- MAGIC results_df=spark.read.option("inferschema",True).json(f"{raw_folder_path}/2021-03-28/results.json")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(results_df)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC results_df.write.format("delta").saveAsTable("f1_demo.results_managed")

-- COMMAND ----------

select * from f1_demo.results_managed;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC results_df.write.format("delta").partitionBy("constructorId").save("abfss://demo@formula1dl1102.dfs.core.windows.net/f1_demo/results_partitioned")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC results_df.write.format("delta").partitionBy("constructorId").mode("overwrite").saveAsTable("f1_demo.results_partitioned")

-- COMMAND ----------

show partitions f1_demo.results_partitioned;

-- COMMAND ----------

select * from f1_demo.results_managed;

-- COMMAND ----------

update f1_demo.results_managed
set points=11-position
where position<=10

-- COMMAND ----------

select * from f1_demo.results_managed;

-- COMMAND ----------


