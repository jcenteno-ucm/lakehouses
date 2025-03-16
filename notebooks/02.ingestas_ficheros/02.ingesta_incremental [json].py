# Databricks notebook source
# MAGIC %md
# MAGIC # Ingesta Incremental
# MAGIC
# MAGIC En este caso vamos a hacer una ingesta incremental batch, esto es, agendada para ejecutarse en momentos concretos

# COMMAND ----------

# MAGIC %md
# MAGIC definimos variables base de los paths de las 3 zonas involucradas en la ingesta

# COMMAND ----------

account = spark.conf.get("adls.account.name")
landing_container = f"abfss://landing@{account}.dfs.core.windows.net"
lakehouse_container = f"abfss://lakehouse@{account}.dfs.core.windows.net"

landing_path = landing_container
raw_path = f"{lakehouse_container}/raw"
bronze_path = f"{lakehouse_container}/bronze"

print(landing_path)
print(raw_path)
print(bronze_path)

# COMMAND ----------

# MAGIC %md
# MAGIC definimos variables base de los paths de las 3 zonas involucradas en la ingesta para este dataset, siguiendo la convención definida

# COMMAND ----------

datasource = 'retail'
dataset = "sales_orders"

dataset_landing_path = f"{landing_path}/{datasource}/{dataset}"
dataset_raw_path =  f"{raw_path}/{datasource}/{dataset}"
dataset_bronze_path = f"{bronze_path}/{datasource}/{dataset}"

print(dataset_landing_path)
print(dataset_raw_path)
print(dataset_bronze_path)

# COMMAND ----------

def list_all_files(path:str):
    """
    Lista recursivamente todos los archivos en una ruta y sus subcarpetas utilizando dbutils.fs.
    
    Parámetros:
        path (str): Ruta base en el sistema de archivos (e.g., 'dbfs:/mnt/mi_carpeta').
    
    Retorna:
        list: Lista de rutas completas de todos los archivos encontrados.
    """
    files_list = []
    try:
        # Listar contenidos de la ruta actual
        items = dbutils.fs.ls(path)
        
        for item in items:
            # Si es un archivo, añadirlo a la lista
            if item.isFile():
                files_list.append(item.path)
            # Si es un directorio, explorar recursivamente
            elif item.isDir():
                files_list.extend(list_all_files(item.path))
    except Exception as e:
        print(f"Error al listar {path}: {str(e)}")
    
    return files_list

for file in list_all_files(dataset_landing_path):
    print(file)

# COMMAND ----------

# MAGIC %md
# MAGIC En este ejercio vamos a hacer una ingesta en modo incremental planificado (scheduled trigger)
# MAGIC
# MAGIC Para ello creamos un Streaming DataFrame usando Databricks autoloader.
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, input_file_name

dataset_schema_location = f'{dataset_bronze_path}_schema'

df = (spark.readStream.format("cloudFiles")
                      .option("cloudFiles.format", "json")
                      .option("cloudFiles.inferColumnTypes", "true")
                      .option("cloudFiles.schemaLocation", dataset_schema_location)
                      .load(dataset_landing_path)
                      .withColumn("_ingested_at",current_timestamp())
                      .withColumn("_ingested_file", input_file_name())
)  

# COMMAND ----------

#display(df)

# COMMAND ----------

spark.sql("CREATE DATABASE IF NOT EXISTS hive_metastore.bronze")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Cuando lancemos el stream en las celdas posteriores, se arrancará y empezará a procesar los ficheros. 
# MAGIC
# MAGIC Tras ingestar todo (ahora mismo solo un fichero) se detendrá. Este comportamiento lo conseguimos mediante el trigger **availableNow**, que es una evolución mejorada del trigger Once en la que procesa poco a poco, todo los ficheros desde la última vez que ejecutó

# COMMAND ----------

(df.writeStream
      .format("delta")
      .trigger(availableNow=True)
      .option("checkpointLocation", f"{dataset_bronze_path}_checkpoint")
      .option("mergeSchema", "true")
      .start(dataset_bronze_path)
)

# COMMAND ----------

display(dbutils.fs.ls(dataset_bronze_path))

# COMMAND ----------

# MAGIC %md
# MAGIC Vamos a comprobar los datos:

# COMMAND ----------

query = f"""
select * 
from delta.`{dataset_bronze_path}`
order by _ingested_at desc
"""
display(spark.sql(query))

# COMMAND ----------

query = f"""
select distinct _ingested_file 
from delta.`{dataset_bronze_path}`
"""
display(spark.sql(query))