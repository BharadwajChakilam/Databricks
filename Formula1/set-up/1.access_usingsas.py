# Databricks notebook source
spark.conf.set("fs.azure.account.auth.type.formula1dl1102.dfs.core.windows.net", "SAS")
spark.conf.set("fs.azure.sas.token.provider.type.formula1dl1102.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.formula1dl1102.dfs.core.windows.net", "sp=rl&st=2024-06-21T17:06:02Z&se=2024-06-22T01:06:02Z&spr=https&sv=2022-11-02&sr=c&sig=eQN7tZ%2FAl7bMnizf8xHE9aGegHgwdjwTtFZ8GT296kM%3D")

# COMMAND ----------

dbutils.fs.ls("abfss://demo@formula1dl1102.dfs.core.windows.net")

# COMMAND ----------

df=spark.read.csv("abfss://demo@formula1dl1102.dfs.core.windows.net/circuits.csv")

# COMMAND ----------

display(df)

# COMMAND ----------

help(dbutils.secrets)

# COMMAND ----------


