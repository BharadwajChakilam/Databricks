-- Databricks notebook source
SHOW DATABASES

-- COMMAND ----------

SELECT current_database()


-- COMMAND ----------

USE f1_processeds

-- COMMAND ----------

show tables

-- COMMAND ----------

select * from f1_processeds.drivers limit 10;

-- COMMAND ----------

desc drivers;

-- COMMAND ----------

select driver_id,driver_ref,dob,name from f1_processeds.drivers
where nationality='British'
and dob>='1990-01-01'
order by  dob desc;

-- COMMAND ----------


