# Databricks notebook source
# MAGIC %md
# MAGIC # Ingesta Multiplex
# MAGIC
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

# MAGIC %md
# MAGIC Leemos la configuración para conectarnos a Kafka

# COMMAND ----------


datasource = "kafka"
dataset = "topics"

dataset_landing_path = f"{landing_path}/{datasource}/{dataset}"
dataset_raw_path =  f"{raw_path}/{datasource}/{dataset}"
dataset_bronze_path = f"{bronze_path}/{datasource}/{dataset}"
dataset_bronze_checkpoint_path = f"{bronze_path}/{datasource}/{dataset}_checkpoint"
table_name = f"hive_metastore.bronze.{datasource}_{dataset}"

print(dataset_landing_path)
print(dataset_raw_path)
print(dataset_bronze_path)

# COMMAND ----------

def read_config():
  config = {}
  with open("/dbfs/FileStore/client_properties") as fh:
    for line in fh:
      line = line.strip()
      if len(line) != 0 and line[0] != "#":
        parameter, value = line.strip().split('=', 1)
        config[parameter] = value.strip()
  return config

conf = read_config()

# COMMAND ----------

# MAGIC %md
# MAGIC En este ejercio vamos a hacer una ingesta streaming desde **kafka** tal y como hicimos con los ficheros.
# MAGIC
# MAGIC Creamos un streaming dataframe con origen kafka. El schema del DataFrame cuando leemos desde kafka está predefinido a la información de un mensaje o registro en Kafka:
# MAGIC
# MAGIC
# MAGIC ```
# MAGIC key:binary
# MAGIC value:binary
# MAGIC topic:string
# MAGIC partition:integer
# MAGIC offset:long
# MAGIC timestamp:timestamp
# MAGIC timestampType:integer
# MAGIC ```
# MAGIC
# MAGIC Resaltar, que tanto el campo **key** como **value** son de tipo **binary**

# COMMAND ----------

import pyspark.sql.functions as F

kafka_options = {
      "kafka.bootstrap.servers": conf["bootstrap.servers"],
      "kafka.security.protocol": conf["security.protocol"],
      "kafka.sasl.mechanism":   conf["sasl.mechanisms"],
      "kafka.sasl.jaas.config":
              f"""kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username="{conf.get('sasl.username')}" password="{conf.get('sasl.password')}"; """,
      "subscribePattern": "[A-Za-z].+",
      "startingOffsets": "earliest"
}


# creamos un streaming dataframe
df = (spark
      .readStream
      .format("kafka") 
      .options(**kafka_options)
      .load()
      .withColumn("ingested_at",F.current_timestamp()) #metadata
)

#renombramos columnas
columns = [F.col(column).alias(f'_{column}') for column in df.columns]
df = df.select(*columns)

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC Como vamos a hacer una ingesta **multiplex**, y todos los topic se ingestarán en una única tabla, no hace falta hacer ningún tratamiento del schema kafka por defecto, ya que es el único schema común a cualquier topic.
# MAGIC
# MAGIC El tratamiento y adaptación de los datos se hará al promocionar los datos a la capa silver

# COMMAND ----------

# MAGIC %md
# MAGIC # Modo Planificado 

# COMMAND ----------

# MAGIC %md
# MAGIC Este sería un ejemplo de ingesta **planificada** o incremental que podríamos agendar por ejemplo de forma horaria. 
# MAGIC
# MAGIC Cada hora ejecutariamos el código y este ingestará todo los datos nuevos desde la ejecución anterior (esto es posible gracias al checkpointing).
# MAGIC
# MAGIC El stream se iniciará y empezará a procesar los eventos del topic y tras ingestar todo se detendrá. Este comportamiento lo conseguimos mediante el trigger **availableNow**, que es una evolución mejorada del trigger **Once**

# COMMAND ----------

(df
  .writeStream
  .partitionBy("_topic")
  .trigger(availableNow=True)
  #.trigger(processingTime="60 seconds") # modo continuo
  .format("delta")
  .outputMode("append")
  .option("path", dataset_bronze_path)
  .option("mergeSchema", "true")
  .option("checkpointLocation", dataset_bronze_checkpoint_path)
  .table(table_name)
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
import pyspark.sql.functions as F

df = (spark.read.format("delta")
        .load(dataset_bronze_path)
        .where("_topic='orders'")
        .orderBy(F.col("_timestamp").desc())
        .limit(10))
        
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC Vamos a comprobar los datos: