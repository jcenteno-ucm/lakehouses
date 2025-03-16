# Databricks notebook source
# MAGIC %md
# MAGIC # Autoloader Planificado
# MAGIC Configuramos la cuenta de almacenamiento (ADLS) en Spark

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

datasource = 'tensorflow'
dataset = "flower_photos"

dataset_landing_path = f"{landing_path}/{datasource}/{dataset}"
dataset_raw_path =  f"{raw_path}/{datasource}/{dataset}"
dataset_bronze_path = f"{bronze_path}/{datasource}/{dataset}"

print(dataset_landing_path)
print(dataset_raw_path)
print(dataset_bronze_path)

# COMMAND ----------

# MAGIC %md 
# MAGIC Es común extraer ciertos metadatados de las imágenes.
# MAGIC * extraer la etiqueta a partir del nombre del fichero
# MAGIC * extraer el tamaño de a imagen

# COMMAND ----------

import io
import numpy as np
import pandas as pd
from pyspark.sql.functions import col, pandas_udf, regexp_extract
from PIL import Image

def extract_label(path_col):
  """Extract label from file path using built-in SQL functions."""
  return regexp_extract(path_col, "flower_photos/([^/]+)", 1)

def extract_size(content):
  """Extract image size from its raw content."""
  image = Image.open(io.BytesIO(content))
  return image.size

@pandas_udf("width: int, height: int")
def extract_size_udf(content_series):
  sizes = content_series.apply(extract_size)
  return pd.DataFrame(list(sizes))

# COMMAND ----------

# MAGIC %md 
# MAGIC Para la ingesta de imágenes haremos uso del sources de ficheros binarios (**binaryFile**)
# MAGIC
# MAGIC Este source convierte cada fichero en un registro individual con el contenido crudo y otros metadatos del fichero.
# MAGIC
# MAGIC Auto Loader procesará de forma eficiente e incremental los ficheros.
# MAGIC
# MAGIC Auto Loader soporta dos modos de descubriento de nuevos ficheros. Este notebook hace uso del modo por defecto (directory listing mode). 
# MAGIC
# MAGIC Además haremos uso del trigger "availableNow".

# COMMAND ----------

import pyspark.sql.functions as f


print(dataset_landing_path)
print(dataset_bronze_path)

bronzeCheckPointLocation = f"{bronze_path}/{datasource}/{dataset}/_checkpoint"

df = (spark.readStream
      .format("cloudFiles")
      .option("cloudFiles.format", "binaryFile")
      .option("recursiveFileLookup", "true")
      .option("pathGlobFilter", "*.jpg")
      .load(dataset_landing_path)
      .withColumn("_size",extract_size_udf(col("content"))) #metadata
      .withColumn("_label",extract_label(col("path"))) #metadata
      .withColumn("_ingested_at",f.current_timestamp()) #metadata
      .withColumn("_ingested_file",f.input_file_name()) #metadata
      .coalesce(1)
)

# el trigger availableNow se dispara solo una vez y tras procesar 
# todo la streaming query se detendrá
(df.writeStream 
  .format("delta")
  .trigger(availableNow=True)
  .option("mergeSchema", "true")
  .option("checkpointLocation", bronzeCheckPointLocation)
  .start(dataset_bronze_path)
)

# COMMAND ----------

# MAGIC %md
# MAGIC Vamos a comprobar los ficheros en la carpeta bronze destino:

# COMMAND ----------

display(dbutils.fs.ls(dataset_bronze_path))

# COMMAND ----------

# MAGIC %md
# MAGIC Vamos a comprobar los datos:

# COMMAND ----------

# DBTITLE 1,Display the data written to the Delta Table
from pyspark.sql.functions import col
display(spark.read.format("delta")
        .load(dataset_bronze_path)
        .orderBy(col("_ingested_at").desc())
        .limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC **Ejercicio**
# MAGIC
# MAGIC Serías capaz de evolucionar la clase LandingStreamReader para trabajar con imágenes además de ficheros json?