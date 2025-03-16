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

topic = "orders_v2"
datasource = "pizzerie"
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

# MAGIC %md
# MAGIC Como vamos a hacer una ingesta **singleplex**, y cada topic se ingestará en una tabla independiente, vamos a escribir el dato en bronze con su estructura completa. 
# MAGIC
# MAGIC Recordemos que en este ejemplo, el campo value está serializado en **avro**, y el campo key es un **string**

# COMMAND ----------

display(df)

# COMMAND ----------

from confluent_kafka.schema_registry import SchemaRegistryClient

# cambiar por las vuestras
schema_registry_url = "https://psrc-j39np.westeurope.azure.confluent.cloud"
schema_registry_username = "TNWAQX6EMYCFU7EI"
schema_registry_password = "e6FVITBSIrcxCwCZN6mY0jzRKVFYN04kYdks72Q3WvEr6F8Gq1zTLIJ0UgtP6hnR"

schema_registry_conf = {'url': schema_registry_url,
                        'basic.auth.user.info' : f'{schema_registry_username}:{schema_registry_password}'}
schema_registry_client = SchemaRegistryClient(schema_registry_conf)

# COMMAND ----------

# MAGIC %md
# MAGIC En este caso, tendremos que recuperar el schema de los datos de este topic mediante el schema registry.

# COMMAND ----------

import pyspark.sql.functions as F
from pyspark.sql.avro.functions import from_avro

value_subject = f"{topic}-value"
value_schema = schema_registry_client.get_latest_version(value_subject).schema.schema_str 

print(value_subject)
print(value_schema)


# COMMAND ----------

df=(df
      #.withColumn("key",F.col("_key").cast("string"))
      .withColumn("value",from_avro(F.expr("substring(_value,6,length(_value)-5)"),value_schema))
      .withColumn("_ingested_at",F.current_timestamp())  #metadata
      .select("*","value.*")
      .drop("value")
)

# COMMAND ----------

display(df)

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

import pyspark.sql.functions as F

df = (spark.read.format("delta")
        .load(dataset_bronze_path)
        .orderBy(F.col("_ingested_at").desc())
        .limit(10))
        
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC **Ejercicio**
# MAGIC
# MAGIC Serías capaz de evolucionar la clase LandingStreamReader para trabajar con kafka y que puede manejar datos serializados en json y avro?