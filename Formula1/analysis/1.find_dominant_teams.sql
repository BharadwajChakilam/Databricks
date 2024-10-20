-- Databricks notebook source
use f1_presentation

-- COMMAND ----------

select * from f1_presentation.calculated_race_results

-- COMMAND ----------

SELECT team_name,count(1) as total_races,sum(calculated_points) as total_points,avg(calculated_points) as avg_points
 FROM f1_presentation.calculated_race_results

 GROUP BY team_name

 ORDER BY avg_points desc


-- COMMAND ----------

SELECT team_name,count(1) as total_races,sum(calculated_points) as total_points,avg(calculated_points) as avg_points
 FROM f1_presentation.calculated_race_results
 where race_year BETWEEN 2011 and 2020
 GROUP BY team_name
 HAVING total_races >=100
 ORDER BY avg_points desc


-- COMMAND ----------

SELECT team_name,count(1) as total_races,sum(calculated_points) as total_points,avg(calculated_points) as avg_points
 FROM f1_presentation.calculated_race_results
 where race_year BETWEEN 2001 and 2011
 GROUP BY team_name
 HAVING total_races >=100
 ORDER BY avg_points desc


-- COMMAND ----------


