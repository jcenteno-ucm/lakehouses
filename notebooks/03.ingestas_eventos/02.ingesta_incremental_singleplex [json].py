# Databricks notebook source
# MAGIC %md
# MAGIC # Ingesta Singleplex
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

datasource = "pizzerie"
topic = "orders"
dataset = topic

dataset_landing_path = f"{landing_path}/{datasource}/{dataset}"
dataset_raw_path =  f"{raw_path}/{datasource}/{dataset}"
dataset_bronze_path = f"{bronze_path}/{datasource}/{dataset}"
dataset_bronze_checkpoint_path = f"{bronze_path}/{datasource}/{dataset}_checkpoint"
table_name = f"hive_metastore.bronze.{datasource}_{dataset}"

print(dataset_landing_path)
print(dataset_raw_path)
print(dataset_bronze_path)

# COMMAND ----------

# MAGIC %md
# MAGIC Leemos la configuración para conectarnos a Kafka

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
# MAGIC Creamos un Streaming Dataframe con origen kafka. El schema del DataFrame cuando leemos desde kafka está predefinido a la información de un mensaje o registro en Kafka:
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
      "subscribe":topic,
      "includeHeaders" : "true",
      "startingOffsets": "earliest"
}

# creamos un streaming dataframe
df = (spark
      .readStream
      .format("kafka") 
      .options(**kafka_options)
      .load()
)
#renombramos columnas
columns = [F.col(column).alias(f'_{column}') for column in df.columns]
df = df.select(*columns)

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC Como vamos a hacer una ingesta **singleplex**, y cada topic se ingestará en una tabla independiente, vamos a escribir el dato en bronze con su estructura completa. 
# MAGIC
# MAGIC Recordemos que en este ejemplo, el campo value es un string que contiene un objeto json, y el campo key también es textual por lo que tendremos que hacer el casting correspondiente

# COMMAND ----------

import pyspark.sql.functions as F

#definimos el schema del dato en formato ddl para poder contruir el objeto json a partir del string
schema="""
  id long, 
  shop string, 
  name string, 
  phoneNumber string, 
  address string,
  pizzas array<struct<pizzaName:string, additionalToppings:array<string>>>
"""  
df=(df
      .withColumn("_ingested_at",F.current_timestamp()) #metadata
      .withColumn("value",F.from_json(F.col("_value").cast("string"),schema))
      .select("*","value.*")
      .drop("value")
)

# COMMAND ----------

display(df)

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

display (spark.read.format("delta")
        .load(dataset_bronze_path)
        .orderBy(F.col("_timestamp").desc())
        .limit(100)
)
        