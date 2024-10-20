-- Databricks notebook source
SELECT team_name,count(1) as total_races,sum(calculated_points) as total_points,avg(calculated_points) as avg_points,
 RANK() over(order by avg(calculated_points) desc) driver_rank
 FROM f1_presentation.calculated_race_results
 where race_year BETWEEN 2011 and 2020
 GROUP BY team_name
 HAVING total_races >=100
 ORDER BY avg_points desc


-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW v_dominant_drivers
as 
SELECT driver_name,count(1) as total_races,sum(calculated_points) as total_points,avg(calculated_points) as avg_points, RANK() over(order by avg(calculated_points) desc) driver_rank
 FROM f1_presentation.calculated_race_results

 GROUP BY driver_name
 HAVING total_races >=50
 ORDER BY avg_points desc


-- COMMAND ----------


SELECT race_year,driver_name,count(1) as total_races,sum(calculated_points) as total_points,avg(calculated_points) as avg_points
 FROM f1_presentation.calculated_race_results
where driver_name in (SELECT driver_name from v_dominant_drivers where driver_rank<=10)
 GROUP BY race_year,driver_name

 ORDER BY race_year,avg_points desc


-- COMMAND ----------


SELECT race_year,driver_name,count(1) as total_races,sum(calculated_points) as total_points,avg(calculated_points) as avg_points
 FROM f1_presentation.calculated_race_results
where driver_name in (SELECT driver_name from v_dominant_drivers where driver_rank<=10)
 GROUP BY race_year,driver_name

 ORDER BY race_year,avg_points desc


-- COMMAND ----------


