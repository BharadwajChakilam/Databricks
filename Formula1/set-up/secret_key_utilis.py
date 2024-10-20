# Databricks notebook source
dbutils.secrets.help()

# COMMAND ----------

dbutils.secrets.listScopes()

# COMMAND ----------

dbutils.secrets.list('formula1scope')

# COMMAND ----------

dbutils.secrets.get(scope='formula1scope',key='formula1dl-ak')

# COMMAND ----------


