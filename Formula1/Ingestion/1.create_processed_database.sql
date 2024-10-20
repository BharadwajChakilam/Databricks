-- Databricks notebook source
CREATE DATABASE IF NOT EXISTS f1_processeds
LOCATION "abfss://processed@formula1dl1102.dfs.core.windows.net/"

-- COMMAND ----------

DESCRIBE DATABASE EXTENDED f1_processeds


-- COMMAND ----------


