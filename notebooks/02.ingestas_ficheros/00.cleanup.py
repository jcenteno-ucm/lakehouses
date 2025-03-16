# Databricks notebook source
# MAGIC %sql
# MAGIC DROP DATABASE IF EXISTS bronze CASCADE;

# COMMAND ----------

account = spark.conf.get("adls.account.name")
landing_path = f"abfss://landing@{account}.dfs.core.windows.net"
lakehouse = f"abfss://lakehouse@{account}.dfs.core.windows.net"

dbutils.fs.rm(landing_path, True)
dbutils.fs.rm(lakehouse, True)