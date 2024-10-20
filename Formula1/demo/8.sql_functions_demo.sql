-- Databricks notebook source
use f1_processeds

-- COMMAND ----------

select *, concat(driver_ref,"-",code) as new_driver_ref from drivers;

-- COMMAND ----------

select *,split(name,' ')[0] as firstname,split(name,' ')[1] as lastname  from drivers

-- COMMAND ----------

select max(dob) from drivers;

-- COMMAND ----------

select * from drivers where dob='1896-12-28'

-- COMMAND ----------


