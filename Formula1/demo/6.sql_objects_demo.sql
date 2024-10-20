-- Databricks notebook source
CREATE DATABASE demo;


-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS demo;


-- COMMAND ----------

SHOW DATABASES;

-- COMMAND ----------

DESCRIBE DATABASE EXTENDED demo;

-- COMMAND ----------

SELECT current_database();

-- COMMAND ----------

SHOW TABLES IN demo;

-- COMMAND ----------

use demo;

-- COMMAND ----------

SELECT current_database();

-- COMMAND ----------

-- MAGIC
-- MAGIC %run "../includes/configuration"

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results_df=spark.read.parquet(f"{presentation_folder_path}/race_results")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results_df.write.format("parquet").saveAsTable("demo.race_results_python")

-- COMMAND ----------

use demo;
show tables;

-- COMMAND ----------

describe extended race_results_python;

-- COMMAND ----------

SELECT * FROM 
DEMO.race_results_python
WHERE race_year=2020;

-- COMMAND ----------

CREATE TABLE race_results_sql
as 
SELECT * FROM 
DEMO.race_results_python
WHERE race_year=2020;

-- COMMAND ----------

desc extended race_results_sql;

-- COMMAND ----------

drop table demo.race_results_sql;

-- COMMAND ----------

show tables in  demo

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results_df.write.format("parquet").option("path",f"{presentation_folder_path}/race_results_ext_py").saveAsTable("demo.race_results_ext_py")

-- COMMAND ----------

DESCRIBE EXTENDED race_results_ext_py;

-- COMMAND ----------

CREATE TABLE demo.race_results_ext_sql
(race_year	int,
race_name	string,
race_date	timestamp,
circuit_location	string,
driver_name	string,
driver_number	int,
driver_nationality	string,
team	string,
grid	bigint,
fastest_lap	string,
race_time	string,
points	double,
position	string,
create_date	timestamp)
using PARQUET
LOCATION "abfss://presentation@formula1dl1102.dfs.core.windows.net/race_results_ext_sql"

-- COMMAND ----------

show tables in demo;

-- COMMAND ----------

INSERT INTO demo.race_results_ext_sql
SELECT * from demo.race_results_ext_py where race_year=2020;

-- COMMAND ----------

select * from demo.race_results_ext_sql

-- COMMAND ----------

Drop table demo.race_results_ext_sql;

-- COMMAND ----------

CREATE or Replace TEMP VIEW v_race_results
as 
SELECT * FROM race_results_python WHERE race_year=2019

-- COMMAND ----------

select * from v_race_results;

-- COMMAND ----------

CREATE or Replace Global TEMP VIEW gv_race_results
as 
SELECT * FROM race_results_python WHERE race_year=2012;

-- COMMAND ----------

select * from global_temp.gv_race_results

-- COMMAND ----------

CREATE or Replace  VIEW demo.pv_race_results
as 
SELECT * FROM race_results_python WHERE race_year=2010;

-- COMMAND ----------

SELECT * FROM demo.pv_race_results

-- COMMAND ----------


