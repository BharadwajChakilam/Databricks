-- Databricks notebook source
DROP DATABASE IF EXISTS f1_processeds CASCADE;

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS f1_processeds
LOCATION "abfss://processed@formula1dl1102.dfs.core.windows.net/race_results_ext_sql"

-- COMMAND ----------

DROP DATABASE IF EXISTS f1_presentation CASCADE;

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS f1_presentation
LOCATION "abfss://presentation@formula1dl1102.dfs.core.windows.net/race_results_ext_sql"

-- COMMAND ----------


