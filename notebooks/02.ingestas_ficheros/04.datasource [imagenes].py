# Databricks notebook source
# MAGIC %md
# MAGIC # Datasource
# MAGIC Para hacer este ejercicio debes crear dos containers en tu cuenta de almacenamiento ADLS configurada (sólo si todavía no los tienes):
# MAGIC
# MAGIC - landing
# MAGIC - lakehouse
# MAGIC
# MAGIC Existen diversas aproximanciones sobre cómo estructurar una data lakehouse usando una o varias cuentas de almacenamiento, containers o directorios
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC A continuación configuramos variables a partir de la cuenta de almacenamiento (ADLS) configurada Spark

# COMMAND ----------

account = spark.conf.get("adls.account.name")
landing_path = f"abfss://landing@{account}.dfs.core.windows.net"

print(landing_path)


# COMMAND ----------

# MAGIC %md
# MAGIC En este caso vamos a trabajar con un dataset de imagenes

# COMMAND ----------

display(dbutils.fs.ls(f"/databricks-datasets/flower_photos/"))

# COMMAND ----------

# MAGIC %sh
# MAGIC ls -la /dbfs/databricks-datasets/flower_photos/roses

# COMMAND ----------

source = 'dbfs:/databricks-datasets/flower_photos/'
target = f'{landing_path}/tensorflow/flower_photos/'
print(source)
print(target)

# COMMAND ----------

dataset = 'roses'

i = 0
for file in dbutils.fs.ls(f'{source}/{dataset}/'):
    dbutils.fs.cp(file.path,file.path.replace(source,target))
    i+=1
    if i > 20:
        break
print('done')