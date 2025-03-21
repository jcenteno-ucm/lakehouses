# Databricks notebook source
# MAGIC %md
# MAGIC #Configuracion Lakehouse

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

bronze_spark_conf = {
    "format" : "delta",
    "options": {
        "mergeSchema": "true"
    },
    "path":bronze_path
}

# COMMAND ----------

# MAGIC %md
# MAGIC # Configuración Kafka

# COMMAND ----------

schema_registry_url = "https://psrc-j39np.westeurope.azure.confluent.cloud"
schema_registry_username = "YL65MHTHKTBH3DEZ"
schema_registry_password = "j2P1hqsAg68hoxKP4diuUl0D8ukHWoJBVmij1DjRMUWuPYIn3ziHfjPGgpTBBP+H"

schema_registry_conf = {'url': schema_registry_url,
                        'basic.auth.user.info' : f'{schema_registry_username}:{schema_registry_password}'}

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

kafka_spark_opts = {
    "kafka.bootstrap.servers" : conf["bootstrap.servers"],
    "kafka.security.protocol": conf["security.protocol"],
    "kafka.sasl.mechanism": conf["sasl.mechanisms"],
    "kafka.sasl.jaas.config": 
              f"""kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username="{conf.get('sasl.username')}" password="{conf.get('sasl.password')}"; """
}

# COMMAND ----------

# MAGIC %md
# MAGIC # Configuración Ingesta

# COMMAND ----------

retail_org_sales = {
    "datasource": "retail",
    "dataset": "sales_orders",    
    "source": {
        "format" : "cloudFiles",
        "options" : {          
            "cloudFiles.format": "json",
            "cloudFiles.inferColumnTypes": "true",
            "cloudFiles.schemaEvolutionMode": "addNewColumns",
            "cloudFiles.schemaHints": "clicked_items string, ordered_products.element.promotion_info string, fulfillment_days string"
        },
        "path": f'{landing_path}/retail/sales_orders'
    },
    "sink": {        
        "layer": "bronze"
    }
}


pizzerie_orders = {
    "datasource": "pizzerie",
    "dataset": "orders",    
    "source": {
        "format" : "kafka",
        "options" : {
            "subscribe" : "orders",
            "startingOffsets" : "earliest"
        },
        "key_format": "string",
        "value_format": "json",
        "json_schema": "id long, shop string, name string, phoneNumber string, address string,pizzas array<struct<pizzaName:string, additionalToppings:array<string>>>"
    },
    "sink": {        
        "layer": "bronze"
    }
}

pizzerie_orders_v2  = {
    "datasource": "pizzerie",
    "dataset": "orders_v2", 
    "source": {
        "format" : "kafka",
        "options" : {
            "subscribe" : "orders_v2",
            "startingOffsets" : "earliest"
        },
        "key_format": "string",
        "value_format": "avro"
    },
    "sink": {        
        "layer": "bronze"
    }
}

ingestion_configs = [retail_org_sales,
                     pizzerie_orders,
                     pizzerie_orders_v2]

# COMMAND ----------

from pyspark.sql import DataFrame
import pyspark.sql.functions as F
from pyspark.sql.avro.functions import from_avro
from confluent_kafka.schema_registry import SchemaRegistryClient

schema_registry_client = SchemaRegistryClient(schema_registry_conf)

def read_stream(ingestion_config:dict) -> DataFrame:
    datasource = ingestion_config.get("datasource")
    dataset = ingestion_config.get("dataset")
    source = ingestion_config.get("source")
    format = source.get("format")
    df = None
    if "kafka" == format:
        opts = kafka_spark_opts.copy()
        opts.update(source.get("options"))
        topic = source.get("options").get("subscribe")
        value_format = source.get("value_format")
        df = (spark.readStream
                .format(format)
                .options(**opts)
                .load())
        df = df.withColumn("key",F.expr("cast(key as string)"))
        if "avro" == value_format:
            value_subject = f"{topic}-value"
            value_schema = schema_registry_client.get_latest_version(value_subject).schema.schema_str 
            df = df.withColumn("value",from_avro(F.expr("substring(value,6,length(value)-5)"),value_schema))
        elif "string" == value_format:
            df = df.withColumn("value",F.expr("cast(value as string)"))
        elif "json" == value_format:
            json_schema = source.get("json_schema")
            df = df.withColumn("value",F.from_json(F.col("value").cast("string"),json_schema))
        else:
            None
    elif "cloudFiles" == format:
        opts = source.get("options")
        landing_path = source.get("path")
        schema_evolution_path = f'{bronze_path}/{datasource}/{dataset}_schema'
        df = (spark.readStream
                .format(format)
                .options(**opts)
                .option("cloudFiles.schemaLocation",schema_evolution_path)
                .load(landing_path)
                .withColumn("_ingested_filename",F.input_file_name())
            )    
    else:
        raise Exception(f'No source format "{format}" implemented!')

    return df.withColumn("_ingested_at",F.current_timestamp()) 



# COMMAND ----------

from pyspark.sql.streaming import StreamingQuery

def write_stream(ingestion_config:dict,df:DataFrame) -> StreamingQuery:
    datasource = ingestion_config.get("datasource")
    dataset = ingestion_config.get("dataset")
    sink = ingestion_config.get("sink")
    layer = sink.get("layer")
    query = None
    if "bronze" == layer:
        format = bronze_spark_conf.get("format")
        opts = bronze_spark_conf.get("options")
        if sink.get("options"):
            opts.update(sink.get("options"))
        query = (df.writeStream
                    .format(format)
                    .options(**opts)
                    .option("checkpointLocation", f'{bronze_path}/{datasource}/{dataset}_checkpoint/')
                    .trigger(availableNow=True)
                    .queryName(f'{datasource} {dataset}')
                    .start(f'{bronze_path}/{datasource}/{dataset}')
                )
    else:
        raise Exception(f'layer {layer} not configured!')
    
    return query  

# COMMAND ----------

#creating dataframes
def read_streams(ingestion_configs:list[dict]) -> list[DataFrame]:
    return [read_stream(c) for c in ingestion_configs]

def write_streams(ingestion_configs:list[dict],dfs:list[DataFrame]) -> list[StreamingQuery]:
    queries = []
    for (c,df) in zip(ingestion_configs,dfs):
        queries.append(write_stream(c,df))
    return queries

# COMMAND ----------

# MAGIC %md
# MAGIC # Motor de Ingesta

# COMMAND ----------


#creating streaming dataframes
dfs = read_streams(ingestion_configs)
#creating streaming queries
queries = write_streams(ingestion_configs, dfs)
#waiting for queries
for q in queries:
  try: 
    q.awaitTermination()
    print(f"🟢 ingestion '{q.name}' completed")
  except:
    print(f"🟠 ingestion '{q.name}' failed")

# COMMAND ----------

