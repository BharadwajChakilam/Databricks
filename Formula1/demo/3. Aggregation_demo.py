# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

race_results_df=spark.read.parquet(f"{presentation_folder_path}/race_results")

# COMMAND ----------

race_results_df


# COMMAND ----------

display(race_results_df)

# COMMAND ----------

demo_df=race_results_df.filter("race_year=2020")
display(demo_df)

# COMMAND ----------

# Assuming demo_df is a DataFrame
demo_df.createOrReplaceTempView("demo_df")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from demo_df
# MAGIC where race_year='2020'

# COMMAND ----------

from pyspark.sql.functions import sum,count,countDistinct


# COMMAND ----------

display(demo_df.select(countDistinct(demo_df.race_name).alias("disitnct_names") ))

# COMMAND ----------

demo_df.filter("driver_name='Lewis Hamilton'").select(sum("points")).show()

# COMMAND ----------

demo_grouped_df=demo_df.groupBy("race_year","driver_name").agg(sum("points").alias("total_points"),countDistinct("race_name").alias("number_of_races")).show()

# COMMAND ----------

demo_df=race_results_df.filter("race_year in (2019,2020)")

# COMMAND ----------

demo_grouped_df=demo_df.groupBy("race_year","driver_name").agg(sum("points").alias("total_points"),countDistinct("race_name").alias("number_of_races"))

# COMMAND ----------

display(demo_grouped_df
        )

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import desc,rank
driverRankSpec=Window.partitionBy("race_year").orderBy(desc("total_points"))
df=demo_grouped_df.withColumn("rank", rank().over(driverRankSpec))

# COMMAND ----------

display(df)

# COMMAND ----------

display(demo_grouped_df)


# COMMAND ----------


