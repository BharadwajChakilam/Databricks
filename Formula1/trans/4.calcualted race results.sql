-- Databricks notebook source
USE f1_processeds

-- COMMAND ----------

CREATE TABLE f1_presentation.calculated_race_results 
using PARQUET 
AS
SELECT races.race_year, constructors.name as team_name,drivers.name as driver_name,results.position,results.points, 11-results.position as calculated_points
FROM results 
join drivers  on (results.driver_id=drivers.driver_id)
join constructors on (results.constructor_id=constructors.constructor_id)
join races on (results.race_id=races.race_id)
where results.position<=10

-- COMMAND ----------

select * from  f1_presentation.calculated_race_results 


-- COMMAND ----------


