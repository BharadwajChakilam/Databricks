# Databricks notebook source
v_result=dbutils.notebook.run("1.ingestion_circuits_file",0,{"p_data_source": "Ergast API"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result=dbutils.notebook.run("2.ingestion_races_file",0,{"p_data_source": "Ergast API"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result=dbutils.notebook.run("3.ingestion_constructors.json_file",0,{"p_data_source": "Ergast API"})

# COMMAND ----------

v_result

# COMMAND ----------



# COMMAND ----------

v_result=dbutils.notebook.run("4.ingestion_drivers.json_file",0,{"p_data_source": "Ergast API"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result=dbutils.notebook.run("5.ingestion_results.json_file",0,{"p_data_source": "Ergast API"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result=dbutils.notebook.run("6.ingestion_pitstops.json_file",0,{"p_data_source": "Ergast API"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result=dbutils.notebook.run("7.ingestion_lap_times_file",0,{"p_data_source": "Ergast API"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result=dbutils.notebook.run("8.ingestion_qualifying_file",0,{"p_data_source": "Ergast API"})

# COMMAND ----------

v_result
