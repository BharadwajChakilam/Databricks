# Databricks notebook source
display(dbutils.fs.ls('/'))

# COMMAND ----------

display(dbutils.fs.ls('/FileStore'))

# COMMAND ----------

df=spark.read.csv('/FileStore/circuits.csv')

# COMMAND ----------

df.show()

# COMMAND ----------

display(df)

# COMMAND ----------


