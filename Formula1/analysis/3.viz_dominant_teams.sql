-- Databricks notebook source
-- MAGIC %python
-- MAGIC html= """<h1 style="color:Black;text-align:center;font-family:Airel">Report on Dominant Formula 1 Drivers </h1>"""
-- MAGIC displayHTML(html)
-- MAGIC

-- COMMAND ----------

SELECT team_name,count(1) as total_races,sum(calculated_points) as total_points,avg(calculated_points) as avg_points,
 RANK() over(order by avg(calculated_points) desc) driver_rank
 FROM f1_presentation.calculated_race_results
 where race_year BETWEEN 2011 and 2020
 GROUP BY team_name
 HAVING total_races >=100
 ORDER BY avg_points desc


-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW v_dominant_teams
as 
SELECT team_name,count(1) as total_races,sum(calculated_points) as total_points,avg(calculated_points) as avg_points, RANK() over(order by avg(calculated_points) desc) team_rank
 FROM f1_presentation.calculated_race_results

 GROUP BY team_name
 HAVING total_races >=100
 ORDER BY avg_points desc

-- COMMAND ----------


SELECT race_year,team_name,count(1) as total_races,sum(calculated_points) as total_points,avg(calculated_points) as avg_points
 FROM f1_presentation.calculated_race_results
where team_name in (SELECT team_name from v_dominant_teams where team_rank<=10)
 GROUP BY race_year,team_name

 ORDER BY race_year,avg_points desc


-- COMMAND ----------


SELECT race_year,team_name,count(1) as total_races,sum(calculated_points) as total_points,avg(calculated_points) as avg_points
 FROM f1_presentation.calculated_race_results
where team_name in (SELECT team_name from v_dominant_teams where team_rank<=10)
 GROUP BY race_year,team_name

 ORDER BY race_year,avg_points desc


-- COMMAND ----------

how to export all visualizations into a dashboard
